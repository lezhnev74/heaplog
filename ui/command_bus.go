package ui

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/go-playground/validator"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"

	"heaplog_2024/db"
)

type CommandBus struct {
	happ       *HeaplogApp
	viewEngine *html.Engine
}

func (bus *CommandBus) Command(c *fiber.Ctx) (err error) {
	commandName := c.Params("cmd")
	switch commandName {
	case "new":
		err = bus.NewQuery(c)
	case "page":
		err = bus.Page(c)
	case "messages_poll":
		err = bus.Messages(c)
	case "pagination_poll":
		err = bus.Pagination(c)
	case "delete":
		err = bus.DeleteQuery(c)
	default:
		err = fmt.Errorf("Unknown command: %s", commandName)
	}

	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	return nil
}

func (bus *CommandBus) DeleteQuery(c *fiber.Ctx) error {
	// 1. Parse page input
	queryId := c.QueryInt("queryId")
	if queryId <= 0 {
		return fmt.Errorf("wrong query id")
	}

	// 2. Make removal
	err := bus.happ.DeleteQuery(queryId)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	return nil
}

// Page renders initial layout for the page of a query (or sub-query).
// This is an entrypoint for new query, opening existing queries, or sub-queries.
func (bus *CommandBus) Page(c *fiber.Ctx) error {
	time.Sleep(time.Second * 2)
	// 1. Parse page input
	queryId := c.QueryInt("queryId")
	if queryId <= 0 {
		return fmt.Errorf("wrong query id")
	}

	page := c.QueryInt("page", 0)
	pageSize := c.QueryInt("pagesize", 100)
	// pageSkip allows to append new results to the same page.
	// pageSkip contains the number of already rendered rows on this page,
	// thus when reading a page from DB, we should skip the first pageSkip rows.
	pageSkip := c.QueryInt("skip", 0)
	// counter how many polls were performed
	polls := c.QueryInt("polls", 0)

	// Respect the sub-query scope
	var from, to *time.Time
	queryFrom := int64(c.QueryInt("from", -1))
	if queryFrom > 0 {
		t := time.UnixMilli(queryFrom)
		from = &t
	}
	queryTo := int64(c.QueryInt("to", -1))
	if queryTo > 0 {
		t := time.UnixMilli(queryTo)
		to = &t
	}

	// 2. Build the query
	query, err := bus.happ.Query(queryId, from, to)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// 3. Fetch page messages
	messages, err := bus.happ.Page(queryId, from, to, page, pageSize, pageSkip)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// 4. Build view models
	viewModel := fiber.Map{}
	// 4.1 Messages View model
	shouldPoll := !query.Finished && len(messages) < (pageSize-pageSkip) // is the page full now?
	pollDelay := fmt.Sprintf("%dms", 500*polls)                          // increase timeouts through time
	viewModel["Messages"] = fiber.Map{
		"QueryId":    queryId,
		"Messages":   messages,
		"ShouldPoll": shouldPoll,
		"Page":       page,
		"PollDelay":  pollDelay,
		"Polls":      polls + 1,
		"NoMessages": query.Finished && query.Messages == 0,
	}
	// 4.2 Pagination View Model
	viewModel["Pagination"] = bus.buildPaginationViewModel(query, page, pageSize)

	// 5. Render HTML
	htmlBuf := bytes.NewBuffer(nil)
	err = bus.viewEngine.Render(htmlBuf, "fragments/page", viewModel)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}
	_, err = c.Write(htmlBuf.Bytes())

	return err
}

// Pagination polls for pagination updates and re-renders.
// THis happens when the query is not finished and pagination must catch up to reflect the current results.
func (bus *CommandBus) Pagination(c *fiber.Ctx) error {

	// 1. Parse page input
	queryId := c.QueryInt("queryId")
	if queryId <= 0 {
		return fmt.Errorf("wrong query id")
	}

	page := c.QueryInt("page", 0)
	pageSize := c.QueryInt("pagesize", 100)

	// Respect the sub-query scope
	var from, to *time.Time
	queryFrom := int64(c.QueryInt("from", -1))
	if queryFrom > 0 {
		t := time.UnixMilli(queryFrom)
		from = &t
	}
	queryTo := int64(c.QueryInt("to", -1))
	if queryTo > 0 {
		t := time.UnixMilli(queryTo)
		to = &t
	}

	// 2. Build the query
	query, err := bus.happ.Query(queryId, from, to)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// 3. Build view models
	viewModel := bus.buildPaginationViewModel(query, page, pageSize)

	// 4. Render HTML
	htmlBuf := bytes.NewBuffer(nil)
	err = bus.viewEngine.Render(htmlBuf, "fragments/pagination", viewModel)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}
	_, err = c.Write(htmlBuf.Bytes())

	return err
}

// Messages polls for messages to append to the current page.
// THis happens when the query is not finished, but the current page is not full.
func (bus *CommandBus) Messages(c *fiber.Ctx) error {

	// 1. Parse page input
	queryId := c.QueryInt("queryId")
	if queryId <= 0 {
		return fmt.Errorf("wrong query id")
	}

	page := c.QueryInt("page", 0)
	pageSize := c.QueryInt("pagesize", 100)
	// pageSkip allows to append new results to the same page.
	// pageSkip contains the number of already rendered rows on this page,
	// thus when reading a page from DB, we should skip the first pageSkip rows.
	pageSkip := c.QueryInt("skip", 0)
	// counter how many polls were performed
	polls := c.QueryInt("polls", 0)

	// Respect the sub-query scope
	var from, to *time.Time
	queryFrom := int64(c.QueryInt("from", -1))
	if queryFrom > 0 {
		t := time.UnixMilli(queryFrom)
		from = &t
	}
	queryTo := int64(c.QueryInt("to", -1))
	if queryTo > 0 {
		t := time.UnixMilli(queryTo)
		to = &t
	}

	// 2. Build the query
	query, err := bus.happ.Query(queryId, from, to)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// 3. Fetch page messages
	messages, err := bus.happ.Page(queryId, from, to, page, pageSize, pageSkip)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// 4. Build view models
	shouldPoll := !query.Finished && len(messages) < (pageSize-pageSkip) // is the page full now?
	pollDelay := fmt.Sprintf("%dms", 5000*polls)                         // increase timeouts through time

	viewModel := fiber.Map{
		"QueryId":    queryId,
		"Messages":   messages,
		"ShouldPoll": shouldPoll,
		"Page":       page,
		"PollDelay":  pollDelay,
		"Polls":      polls + 1,
		"NoMessages": query.Finished && query.Messages == 0,
	}

	// 5. Render HTML
	htmlBuf := bytes.NewBuffer(nil)
	err = bus.viewEngine.Render(htmlBuf, "fragments/messages", viewModel)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}
	_, err = c.Write(htmlBuf.Bytes())

	return err
}

func (bus *CommandBus) buildPaginationViewModel(query db.Query, page, pageSize int) fiber.Map {
	pages := int(math.Ceil(float64(query.Messages) / float64(pageSize)))
	pageNext := page + 1
	pagePrev := page - 1
	PageNextExists := true
	if (page + 1) >= pages {
		PageNextExists = false
	}
	PagePrevExists := true
	if page <= 0 {
		PagePrevExists = false
	}

	pagesArray := make([]int, pages)
	for i := 0; i < pages; i++ {
		pagesArray[i] = i
	}

	return fiber.Map{
		"QueryId":         query.Id,
		"Pages":           pages,
		"PagesArray":      pagesArray,
		"Page":            page,
		"VisiblePage":     page + 1,
		"PageSize":        pageSize,
		"PageNextExists":  PageNextExists,
		"PageNext":        pageNext,
		"PagePrevExists":  PagePrevExists,
		"PagePrev":        pagePrev,
		"DocCount":        query.Messages,
		"QueryInProgress": !query.Finished,
		"Top":             true,
	}
}

// NewQuery builds a new query.
func (bus *CommandBus) NewQuery(c *fiber.Ctx) error {
	// Read body payload
	type InputQuery struct {
		Text     string `validate:"required"`
		From, To string `validate:"date"`
	}
	input := new(InputQuery)
	err := c.QueryParser(input)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// Validate payload + mapping dates
	dates := make(map[string]*time.Time)
	dates["From"] = nil
	dates["To"] = nil
	v := validator.New()
	err = v.RegisterValidation("date", func(fl validator.FieldLevel) bool {
		s := fl.Field().String()
		if s == "" {
			return true // optional validation
		}
		t, err := time.Parse("02.01.2006 15:04:05", s) // UTC time
		if err != nil {
			return false
		}
		dates[fl.FieldName()] = &t
		return true
	}, false)
	if err != nil {
		message := fmt.Sprintf("Validation failed: %s\n", err)
		return &fiber.Error{Code: fiber.StatusOK, Message: message}
	}

	err = v.Struct(input)
	if err != nil {
		message := "Invalid request payload:\n"
		for _, err := range err.(validator.ValidationErrors) {
			var ie InvalidInput
			ie.Field = err.StructField()
			message += fmt.Sprintf(`"%v" field is invalid (%v)\n`, err.StructField(), err.ActualTag())
		}
		return &fiber.Error{Code: fiber.StatusOK, Message: message}
	}
	if dates["From"] != nil && dates["To"] != nil && dates["To"].Before(*dates["From"]) {
		return &fiber.Error{Code: fiber.StatusOK, Message: "date 'to' > date 'from'"}
	}

	// Now run the query and instantly return with the query Id
	query, _, err := bus.happ.NewQuery(input.Text, dates["From"], dates["To"])
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	c.Response().Header.Add("Hx-Push-Url", fmt.Sprintf("/query/%d", query.Id))
	viewModel := fiber.Map{
		"QueryId": query.Id,
	}
	htmlBuf := bytes.NewBuffer(nil)
	err = bus.viewEngine.Render(htmlBuf, "fragments/query_load", viewModel)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}
	_, err = c.Write(htmlBuf.Bytes())

	return err
}

//func (bus *CommandBus) pushFragment(c *fiber.Ctx, targetHtmlId, fragmentName string, payload fiber.Map) error {
//	fragmentHtml, err := bus.buildFragment(targetHtmlId, fragmentName, payload)
//	if err != nil {
//		return err
//	}
//	_, err = c.Write(fragmentHtml)
//	return err
//}
//
//func (bus *CommandBus) buildFragment(targetHtmlId, fragmentName string, payload fiber.Map) (fragment []byte, err error) {
//	tplBuf := bytes.NewBuffer(nil)
//	err = bus.viewEngine.Render(tplBuf, fragmentName, payload)
//	if err != nil {
//		return nil, xerrors.Errorf("unable to render a fragment %s: %w", fragmentName, err)
//	}
//	fragmentString := fmt.Sprintf("<div id=\"%s\" hx-swap-oob=\"true\">\n%s\n</div>", targetHtmlId, tplBuf.Bytes())
//	return []byte(fragmentString), nil
//}
