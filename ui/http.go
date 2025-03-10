package ui

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/gofiber/template/html/v2"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

//go:embed web_templates/*
var webTemplate embed.FS

//go:embed web_static
var webStatic embed.FS

type InvalidInput struct {
	Field string
}

func defaultErrorHandler(ctx *fiber.Ctx, err error) error {
	// Status code defaults to 500
	code := fiber.StatusInternalServerError

	// Retrieve the custom status code if it's a *fiber.Error
	var e *fiber.Error
	if errors.As(err, &e) {
		code = e.Code
	}
	ctx.Status(code)

	// Send custom error page
	return ctx.Render("error", fiber.Map{
		"Error": fmt.Sprintf("%s", err),
	})
}

func makeHttpApp(happ *HeaplogApp, viewsDirectory string) *fiber.App {

	subFS, _ := fs.Sub(webTemplate, "web_templates") // that is to not include the parent directory to the tpl paths
	viewsEngine := html.NewFileSystem(http.FS(subFS), ".gohtml")
	// DEBUGGING:
	// viewsEngine = html.New(viewsDirectory, ".gohtml")
	// viewsEngine.Reload(true)
	viewFuncs := map[string]any{
		"Inc": func(arg1 any) int {
			n := arg1.(int)
			return n + 1
		},
		// count \n for messages
		"lines": func(m any) (int, error) {
			s := m.(string)
			return strings.Count(s, "\n"), nil
		},
		"shortNumber": func(m any) (string, error) {
			c := m.(int)
			s := fmt.Sprintf("%d", c)
			if c > 999 {
				s = fmt.Sprintf("%.1fK", float64(c)/1000)
				s = strings.TrimSuffix(s, ".0") // trim round suffix
			}
			return s, nil
		},
		// extend the map in the first argument with values
		"extendMap": func(m any, pairs ...any) (fiber.Map, error) {
			if len(pairs)%2 != 0 {
				return nil, errors.New("misaligned with")
			}

			target, ok := m.(fiber.Map)
			if !ok {
				return nil, fmt.Errorf("the first argument must be a map, %T given", m)
			}

			// clone the map to create a new environment
			targetCopy := make(fiber.Map, len(target)+len(pairs)/2)
			maps.Copy(targetCopy, target)

			for i := 0; i < len(pairs); i += 2 {
				key, ok := pairs[i].(string)

				if !ok {
					return nil, fmt.Errorf("cannot use type %T as map key", pairs[i])
				}
				targetCopy[key] = pairs[i+1]
			}

			return targetCopy, nil
		},
	}
	viewsEngine.AddFuncMap(viewFuncs)

	httpApp := fiber.New(fiber.Config{
		Views:                 viewsEngine,
		ErrorHandler:          defaultErrorHandler,
		DisableStartupMessage: true,
	})
	c := cors.ConfigDefault
	c.ExposeHeaders = "*"
	httpApp.Use(cors.New(c))
	httpApp.Use(compress.New())
	// httpApp.Use(cache.New(cache.Config{
	// 	Expiration: 30 * time.Minute,
	// }))

	// HTTP ROUTES:
	httpApp.Use("/static", filesystem.New(filesystem.Config{
		Root:       http.FS(webStatic),
		PathPrefix: "web_static",
		Browse:     false,
	}))
	httpApp.Get("/query/:queryId", func(c *fiber.Ctx) error {
		queryId, err := c.ParamsInt("queryId")
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}
		query, err := happ.Query(queryId, nil, nil)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}

		viewModel := fiber.Map{
			"QueryId": queryId,
			"Page":    c.QueryInt("page", 1) - 1,
			// Search Form:
			"Query": query.Text,
			"From":  "",
			"To":    "",
		}

		if query.Min != nil {
			viewModel["From"] = query.Min.UTC().Format("02.01.2006 15:04:05")
		}
		if query.Max != nil {
			viewModel["To"] = query.Max.UTC().Format("02.01.2006 15:04:05")
		}

		return c.Render("query", viewModel, "theme")
	})
	httpApp.Get("/", func(c *fiber.Ctx) error {
		// read existing old queries
		list, err := happ.ListQueries()
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}

		qData := make([]fiber.Map, 0)
		for _, q := range list {
			data := fiber.Map{
				"QueryId": q.Id,
				"Query":   q.Text,
				"Count":   q.Messages,
				"From":    "",
				"To":      "",
			}

			if q.Min != nil {
				data["From"] = q.Min.UTC().Format("02.01.2006 15:04:05")
			}
			if q.Max != nil {
				data["To"] = q.Max.UTC().Format("02.01.2006 15:04:05")
			}
			qData = append(qData, data)
		}

		return c.Render("list", fiber.Map{"Queries": qData}, "theme")
	})
	// This is the main gateway to command bus, commands come from UI.
	// The bus executes the command and accumulates HTML changes to apply to the UI (via htmx).
	bus := &CommandBus{happ, viewsEngine}
	httpApp.Get("/cmd/:cmd", func(c *fiber.Ctx) error { return bus.Command(c) })

	return httpApp
}
