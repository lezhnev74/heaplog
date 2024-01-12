package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/gofiber/template/html/v2"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"heaplog/heaplog"
	"io/fs"
	"log"
	"math"
	"net/http"
	"sort"
	"time"
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

func makeFiber(happ *heaplog.Heaplog, viewsDirectory string) *fiber.App {

	subFS, _ := fs.Sub(webTemplate, "web_templates") // that is to not include the parent directory to the tpl paths
	viewsEngine := html.NewFileSystem(http.FS(subFS), ".gohtml")

	// DEBUGGING:
	viewsEngine = html.New(viewsDirectory, ".gohtml")
	viewsEngine.Reload(true)

	viewFuncs := map[string]any{
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

	app := fiber.New(fiber.Config{
		Views:                 viewsEngine,
		ErrorHandler:          defaultErrorHandler,
		DisableStartupMessage: true,
	})

	c := cors.ConfigDefault
	c.ExposeHeaders = "*"
	app.Use(cors.New(c))

	app.Use("/static", filesystem.New(filesystem.Config{
		Root:       http.FS(webStatic),
		PathPrefix: "web_static",
		Browse:     false,
	}))

	app.Get("/", func(c *fiber.Ctx) error {
		// read existing old queries
		s, err := happ.AllQueriesSummaries()
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}

		qData := make([]fiber.Map, 0)
		for _, querySummary := range s {
			data := fiber.Map{
				"QueryId": querySummary.QueryId,
				"Query":   querySummary.Text,
				"Count":   querySummary.Total,
				"From":    "",
				"To":      "",
			}

			if querySummary.From != nil {
				data["From"] = querySummary.From.UTC().Format("02.01.2006 15:04:05")
			}
			if querySummary.To != nil {
				data["To"] = querySummary.To.UTC().Format("02.01.2006 15:04:05")
			}
			qData = append(qData, data)
		}

		return c.Render("home", fiber.Map{"Queries": qData}, "theme")
	})

	app.Get("/query/:queryId", func(c *fiber.Ctx) error {
		queryId := c.Params("queryId")
		querySummary, err := happ.QuerySummary(queryId, nil, nil)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}

		data := fiber.Map{
			"QueryId": c.Params("queryId"),
			"Query":   querySummary.Text,
			"From":    "",
			"To":      "",
			"Page":    c.QueryInt("page", 1) - 1,
		}

		if querySummary.From != nil {
			data["From"] = querySummary.From.UTC().Format("02.01.2006 15:04:05")
		}
		if querySummary.To != nil {
			data["To"] = querySummary.To.UTC().Format("02.01.2006 15:04:05")
		}

		return c.Render("home", data, "theme")
	})

	app.Get("/query/:queryId/aggregate", func(c *fiber.Ctx) error {
		queryId := c.Params("queryId")
		querySummary, err := happ.QuerySummary(queryId, nil, nil)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}

		discretization := c.QueryInt("discretization", 10)
		from := *querySummary.MinDoc // default
		to := *querySummary.MaxDoc   // default

		queryFrom := int64(c.QueryInt("from", 0))
		if queryFrom > from.UnixMilli() {
			from = time.UnixMilli(queryFrom)
		}
		queryTo := int64(c.QueryInt("to", math.MaxInt64))
		if queryTo < to.UnixMilli() {
			to = time.UnixMilli(queryTo)
		}

		timeline, err := happ.AggregatePage(queryId, discretization, from, to)

		// convert a map int->int to {x:int, y:int}
		type point struct {
			X int64 `json:"x"`
			Y int64 `json:"y"`
		}
		points := make([]point, 0, len(timeline))
		for k, v := range timeline { // unsorted
			points = append(points, point{k, v})
		}
		sort.Slice(points, func(i, j int) bool {
			a, b := points[i], points[j]
			return a.X < b.X
		})

		jsonList, err := json.Marshal(points)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}

		return c.SendString(string(jsonList))
	})

	app.Get("/cmd/:cmd", func(c *fiber.Ctx) error { return NewHtmxController(happ, viewsEngine).CommandFiber(c) })

	return app
}

func StartWebServer(happ *heaplog.Heaplog, viewsDirectory string) {
	app := makeFiber(happ, viewsDirectory)
	log.Printf("Listening on port 8393")
	log.Fatal(app.Listen(":8393"))
}
