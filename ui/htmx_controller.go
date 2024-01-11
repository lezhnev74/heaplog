package ui

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/go-playground/validator"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
	"golang.org/x/xerrors"
	"heaplog/common"
	"heaplog/heaplog"
	"heaplog/storage"
	"io"
	"math"
	"time"
)

type HtmxController struct {
	happ       *heaplog.Heaplog
	viewEngine *html.Engine
}

// CommandFiber accepts a command and receives the resul to the Fiber object.
func (hc *HtmxController) CommandFiber(c *fiber.Ctx) (err error) {
	commandName := c.Params("cmd")

	t := time.Now()

	switch commandName {
	case "new":
		err = hc.CommandNew(c)
	case "page":
		err = hc.CommandPage(c)
	case "checkComplete":
		err = hc.CommandPaginationCheckComplete(c)
	default:
		err = fmt.Errorf("Unknown command received: %s", commandName)
	}

	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	d := time.Now().Sub(t)
	cmdOut := fmt.Sprintf("<div>Last command took %0.3fs</div>", d.Seconds())
	c.Response().BodyWriter().Write([]byte(cmdOut))

	c.Set("HX-Retarget", "#command_out")
	return nil
}

// CommandNew is a new search request came from the search form.
func (hc *HtmxController) CommandNew(c *fiber.Ctx) error {
	// Read body payload
	type InputQuery struct {
		Text     string `validate:"required"`
		From, To string `validate:"date"`
		Pagesize int
	}
	input := new(InputQuery)
	err := c.QueryParser(input)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}
	if input.Pagesize == 0 {
		input.Pagesize = 100
	}

	// Validate payload + mapping dates
	dates := make(map[string]*time.Time)
	dates["From"] = nil
	dates["To"] = nil
	v := validator.New()
	v.RegisterValidation("date", func(fl validator.FieldLevel) bool {
		s := fl.Field().String()
		if s == "" {
			return true // optional validation
		}
		t, err := time.Parse("02.01.2006 15:04:05", s) // expect UTC
		if err != nil {
			return false
		}
		dates[fl.FieldName()] = &t
		return true
	}, false)
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

	// Build Query
	var queryId string
	// test.Profile(func() {
	queryId, err = hc.happ.NewQuery(input.Text, input.Pagesize, dates["From"], dates["To"]) // potentially long operation
	// })
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// Render Page
	var querySummary common.QuerySummary
	for i := 0; i < 3; i++ {
		querySummary, err = hc.happ.QuerySummary(queryId, nil, nil)
		if errors.Is(err, storage.ErrNoData) || querySummary.Total == 0 {
			// wait a bit until matched messages are flushed to the storage
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	if querySummary.Total == 0 {
		// Empty result set:
		err = hc.putFragment(c.Response().BodyWriter(), "query_results", "fragments/query_no_results", nil)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}
	} else {
		err = hc.putResultsPage(c.Response().BodyWriter(), queryId, querySummary, 0, input.Pagesize, nil, nil, true)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}
	}

	c.Response().Header.Add("Hx-Push-Url", fmt.Sprintf("/query/%s", queryId))

	return nil
}

func (hc *HtmxController) CommandPage(c *fiber.Ctx) error {

	queryId := c.Query("queryId")
	page := c.QueryInt("page", 0)
	pageSize := c.QueryInt("pagesize", 100)
	freshLoad := c.QueryBool("freshLoad", false)

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

	querySummary, err := hc.happ.QuerySummary(queryId, from, to)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	if querySummary.Total == 0 {
		// Empty result set:
		err = hc.putFragment(c.Response().BodyWriter(), "query_results", "fragments/query_no_results", nil)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}
	} else {
		err = hc.putResultsPage(c.Response().BodyWriter(), queryId, querySummary, page, pageSize, from, to, freshLoad)
		if err != nil {
			return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
		}
	}

	c.Response().Header.Add("Hx-Push-Url", fmt.Sprintf("/query/%s?page=%d", queryId, page+1))

	return nil
}

func (hc *HtmxController) CommandPaginationCheckComplete(c *fiber.Ctx) error {
	queryId := c.Query("queryId")
	page := c.QueryInt("page", 0)
	pageSize := c.QueryInt("pagesize", 100)

	querySummary, err := hc.happ.QuerySummary(queryId, nil, nil)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	// Pages math:
	pages := int(math.Ceil(float64(querySummary.Total) / float64(pageSize)))
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

	PageData := fiber.Map{
		// query:
		"QueryId": queryId,
		// results:
		"PageSize":        pageSize,
		"Pages":           pages,
		"Page":            page,
		"VisiblePage":     page + 1,
		"PageNextExists":  PageNextExists,
		"PageNext":        pageNext,
		"PagePrevExists":  PagePrevExists,
		"PagePrev":        pagePrev,
		"DocCount":        querySummary.Total,
		"QueryInProgress": !querySummary.Complete,

		"Top": false,
	}

	err = hc.putFragment(c.Response().BodyWriter(), "pagination", "fragments/pagination", PageData)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	PageData["Top"] = true
	err = hc.putFragment(c.Response().BodyWriter(), "pagination_top", "fragments/pagination", PageData)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	PageData["FromMilli"] = querySummary.MinDoc.UnixMilli()
	PageData["ToMilli"] = querySummary.MaxDoc.UnixMilli()
	return hc.putFragment(c.Response().BodyWriter(), "secondary_area", "fragments/timeline", PageData)
}

// page starts from 0
func (hc *HtmxController) putResultsPage(
	out io.Writer,
	queryId string,
	querySummary common.QuerySummary,
	page int,
	pageSize int,
	from, to *time.Time,
	loadTimeline bool,
) error {

	// Read docs:
	messages, err := hc.happ.QueryPage(queryId, page, pageSize, from, to)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}
	messagesStrings := make([]string, 0, len(messages))
	for _, m := range messages {
		ms := string(m)
		messagesStrings = append(messagesStrings, ms)
	}

	// Pages math:
	pages := int(math.Ceil(float64(querySummary.Total) / float64(pageSize)))
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

	PageData := fiber.Map{
		// query:
		"QueryId": queryId,
		// results:
		"PageSize":        pageSize,
		"Pages":           pages,
		"Page":            page,
		"VisiblePage":     page + 1,
		"PageNextExists":  PageNextExists,
		"PageNext":        pageNext,
		"PagePrevExists":  PagePrevExists,
		"PagePrev":        pagePrev,
		"Docs":            messagesStrings,
		"DocCount":        querySummary.Total,
		"QueryInProgress": !querySummary.Complete,
	}

	err = hc.putFragment(out, "query_results", "fragments/results", PageData)
	if err != nil {
		return &fiber.Error{Code: fiber.StatusOK, Message: err.Error()}
	}

	if loadTimeline {
		PageData["FromMilli"] = querySummary.MinDoc.UnixMilli()
		PageData["ToMilli"] = querySummary.MaxDoc.UnixMilli()
		return hc.putFragment(out, "secondary_area", "fragments/timeline", PageData)
	}

	return nil
}

func (hc *HtmxController) putFragment(out io.Writer, targetHtmlId, fragmentName string, payload fiber.Map) error {
	tplBuf := bytes.NewBuffer(nil)
	err := hc.viewEngine.Render(tplBuf, fragmentName, payload)
	if err != nil {
		return xerrors.Errorf("unable to render a fragment %s: %w", fragmentName, err)
	}

	_, err = fmt.Fprintf(out, "<div id=\"%s\" hx-swap-oob=\"true\">\n%s\n</div>", targetHtmlId, tplBuf.Bytes())
	if err != nil {
		return xerrors.Errorf("unable to render a fragment %s: %w", fragmentName, err)
	}
	return err
}

func NewHtmxController(happ *heaplog.Heaplog, viewEngine *html.Engine) *HtmxController {
	return &HtmxController{
		happ:       happ,
		viewEngine: viewEngine,
	}
}
