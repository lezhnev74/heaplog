package ui

import (
	"context"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

// SveltePagePayload allows server-side navigation flow, it addresses a page to be opened on FE with its payload(props).
type SveltePagePayload struct {
	Component string      `json:"component"`
	Props     interface{} `json:"props"`
}

func NewHttpApp(ctx context.Context, frontendPublic http.FileSystem, heaplog Heaplog) *fiber.App {
	app := fiber.New(
		fiber.Config{
			DisableStartupMessage: true,
		},
	)
	go func() {
		<-ctx.Done()
		app.Shutdown()
	}()

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

			expr, err := query_language.ParseUserQuery(req.Query)
			if err != nil {
				heaplog.Logger.Warn("could not parse query", zap.Error(err))
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Bad query syntax.",
					},
				)
			}

			messages, err := heaplog.Searcher.Search(expr, from, to)
			if err != nil {
				heaplog.Logger.Warn("search failed", zap.Error(err))
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Search failed",
					},
				)
			}

			query := common.UserQuery{
				Query:    req.Query,
				FromDate: from,
				ToDate:   to,
			}
			r, _, err := heaplog.Results.PutResultsAsync(query, common.ToFileMessages(messages))
			if err != nil {
				heaplog.Logger.Warn("search results failed", zap.Error(err))
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Search failed",
					},
				)
			}

			var fromMicro, toMicro int64
			if r.FromDate != nil {
				fromMicro = r.FromDate.UnixMicro()
			}
			if r.ToDate != nil {
				toMicro = r.ToDate.UnixMicro()
			}

			payload := SveltePagePayload{
				Component: "Query",
				Props: map[string]interface{}{
					"id":    r.Id,
					"query": r.Query,
					"from":  fromMicro,
					"to":    toMicro,
				},
			}

			return c.JSON(payload)
		},
	)

	return app
}
