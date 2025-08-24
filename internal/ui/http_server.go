package ui

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/gofiber/template/html/v2"
	"go.uber.org/zap"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/search/query_language"
)

// SveltePagePayload allows server-side navigation flow, it addresses a page to be opened on FE with its payload(props).
type SveltePagePayload struct {
	Component string `json:"component"`
	Props     any    `json:"props"`
}

func NewHttpApp(ctx context.Context, frontendPublic http.FileSystem, heaplog Heaplog) *fiber.App {
	engine := html.NewFileSystem(frontendPublic, ".html")
	app := fiber.New(
		fiber.Config{
			DisableStartupMessage: true,
			Views:                 engine,
		},
	)

	c := cors.ConfigDefault
	c.ExposeHeaders = "*"
	app.Use(cors.New(c))
	app.Use(compress.New(compress.Config{Level: compress.LevelBestCompression}))
	app.Use(
		"/assets", filesystem.New(
			filesystem.Config{
				Root:       frontendPublic,
				PathPrefix: "frontend/public/assets",
				Browse:     false,
			},
		),
	)

	app.Get(
		"/", func(c *fiber.Ctx) error {
			// List all queries
			results, err := heaplog.Results.GetResults(nil)
			if err != nil {
				heaplog.Logger.Error("failed to get results", zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(
					fiber.Map{
						"error": "Error.",
					},
				)
			}

			list := append([]*common.SearchResult{}, slices.Collect(maps.Values(results))...)
			slices.SortFunc(
				list, func(a, b *common.SearchResult) int {
					return b.CreatedAt.Compare(a.CreatedAt)
				},
			)

			payload := SveltePagePayload{
				Component: "Home",
				Props: map[string]any{
					"queries": list,
				},
			}

			// Render index.html, injecting the payload
			return c.Render(
				"frontend/public/index", fiber.Map{
					"InitialPage": payload,
				},
			)
		},
	)

	app.Get(
		"/query/:id", func(c *fiber.Ctx) error {

			id, err := c.ParamsInt("id")
			if err != nil {
				heaplog.Logger.Error("failed to parse query id", zap.Error(err))
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Invalid query ID.",
					},
				)
			}

			// List all queries
			results, err := heaplog.Results.GetResults([]int{id})
			if err != nil {
				heaplog.Logger.Error("failed to get results", zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(
					fiber.Map{
						"error": "Error.",
					},
				)
			}

			payload := SveltePagePayload{
				Component: "NotFound",
				Props:     map[string]any{},
			}
			if results[id] != nil {
				payload = SveltePagePayload{
					Component: "Query",
					Props:     results[id],
				}
			}

			// Render index.html, injecting the payload
			return c.Render(
				"frontend/public/index", fiber.Map{
					"InitialPage": payload,
				},
			)
		},
	)

	app.Get(
		"/api/query", func(c *fiber.Ctx) error {
			// List all queries
			results, err := heaplog.Results.GetResults(nil)
			if err != nil {
				heaplog.Logger.Error("failed to get results", zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(
					fiber.Map{
						"error": "Error.",
					},
				)
			}

			list := append([]*common.SearchResult{}, slices.Collect(maps.Values(results))...)
			slices.SortFunc(
				list, func(a, b *common.SearchResult) int {
					return b.CreatedAt.Compare(a.CreatedAt)
				},
			)

			return c.Status(fiber.StatusOK).JSON(
				fiber.Map{
					"queries": list,
				},
			)
		},
	)

	app.Get(
		"/api/query/:id", func(c *fiber.Ctx) error {
			id, err := c.ParamsInt("id")
			if err != nil {
				heaplog.Logger.Error("failed to parse query id", zap.Error(err))
				return c.Status(fiber.StatusBadRequest).JSON(
					fiber.Map{
						"error": "Invalid query ID.",
					},
				)
			}
			skip := c.QueryInt("skip", 0)
			limit := c.QueryInt("limit", 100)

			// List all queries
			resultsMap, err := heaplog.Results.GetResults(nil)
			if err != nil {
				heaplog.Logger.Error("failed to get results", zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(
					fiber.Map{
						"error": "Error.",
					},
				)
			}
			query := resultsMap[id]

			// List all queries
			results, err := heaplog.Results.GetResultMessages(id, skip, limit)
			if err != nil {
				heaplog.Logger.Error("failed to get results", zap.Error(err))
				return c.Status(fiber.StatusInternalServerError).JSON(
					fiber.Map{
						"error": "Error.",
					},
				)
			}

			pool := common.NewBufferPool([]int{1000})
			bodies := func(yield func(string) bool) {
				for mf, err := range common.ReadMessages(ctx, pool, results) {
					if err != nil {
						yield("read message failed:" + err.Error())
						break
					}
					if !yield(string(mf.Body)) {
						break
					}
				}
			}

			return c.JSON(
				fiber.Map{
					"query":    query,
					"messages": append([]string{}, slices.Collect(bodies)...),
				},
			)
		},
	)

	app.Post(
		"/api/query", func(c *fiber.Ctx) error {

			type QueryRequest struct {
				Query string `json:"query"`
				From  string `json:"fromDate"`
				To    string `json:"toDate"`
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

			payload := SveltePagePayload{
				Component: "Query",
				Props:     r,
			}

			return c.JSON(payload)
		},
	)

	return app
}
