package ui

import (
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
)

type SveltePagePayload struct {
	Component string      `json:"component"`
	Props     interface{} `json:"props"`
}

func NewHttpApp(frontendPublic http.FileSystem, heaplog Heaplog) *fiber.App {
	app := fiber.New(
		fiber.Config{
			DisableStartupMessage: true,
		},
	)
	c := cors.ConfigDefault
	c.ExposeHeaders = "*"
	app.Use(cors.New(c))
	app.Use(compress.New(compress.Config{Level: compress.LevelBestCompression}))
	app.Use(
		"/", filesystem.New(
			filesystem.Config{
				Root:       frontendPublic,
				PathPrefix: "frontend/public",
				Browse:     false,
			},
		),
	)

	api := app.Group("/api")
	api.Get(
		"/", func(c *fiber.Ctx) error {
			// List all queries
			return c.SendString("Hello, World!")
		},
	)
	api.Post(
		"/query", func(c *fiber.Ctx) error {

			type QueryRequest struct {
				Query string `json:"query"`
				From  string `json:"from"`
				To    string `json:"to"`
			}

			var (
				req      QueryRequest
				from, to *time.Time
			)

			if err := c.BodyParser(&req); err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Invalid request body",
					},
				)
			}
			if req.Query == "" {
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Query is empty",
					},
				)
			}

			if req.From != "" {
				d, err := time.Parse(time.RFC3339, req.From)
				if err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(
						fiber.Map{
							"error": "Invalid 'from' date format.",
						},
					)
				}
				from = &d
			}

			if req.To != "" {
				d, err := time.Parse(time.RFC3339, req.To)
				if err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(
						fiber.Map{
							"error": "Invalid 'to' date format.",
						},
					)
				}
				to = &d
			}

			if from != nil && to != nil {
				if from.After(*to) {
					return c.Status(fiber.StatusBadRequest).JSON(
						fiber.Map{
							"error": "dates are not ordered",
						},
					)
				}
			}

			payload := SveltePagePayload{
				Component: "Query",
				Props: map[string]interface{}{
					"id":    "99",
					"query": "q99999",
				},
			}

			return c.JSON(payload)
		},
	)

	return app
}
