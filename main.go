package main

import (
	"embed"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"golang.org/x/exp/rand"
)

//go:embed frontend/public
var frontendPublic embed.FS

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
	return ctx.Render(
		"error", fiber.Map{
			"Error": fmt.Sprintf("%s", err),
		},
	)
}

func main() {
	httpApp := fiber.New(
		fiber.Config{
			ErrorHandler:          defaultErrorHandler,
			DisableStartupMessage: true,
		},
	)
	c := cors.ConfigDefault
	c.ExposeHeaders = "*"
	httpApp.Use(cors.New(c))
	httpApp.Use(compress.New(compress.Config{Level: compress.LevelBestCompression}))

	// HTTP ROUTES:
	httpApp.Use(
		"/", filesystem.New(
			filesystem.Config{
				Root:       http.FS(frontendPublic),
				PathPrefix: "frontend/public",
				Browse:     false,
			},
		),
	)
	httpApp.Use(
		"/query", filesystem.New(
			filesystem.Config{
				Root:       http.FS(frontendPublic),
				PathPrefix: "frontend/public",
			},
		),
	)

	api := httpApp.Group("/api")
	api.Get(
		"/random", func(c *fiber.Ctx) error {
			return c.JSON(
				fiber.Map{"random": fmt.Sprintf(
					"%d",
					rand.New(rand.NewSource(uint64(time.Now().UnixNano()))).Int63(),
				)},
			)
		},
	)

	httpApp.Listen(":3000")
}
