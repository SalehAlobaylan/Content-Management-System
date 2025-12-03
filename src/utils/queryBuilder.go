package utils

/*
*
* Query Builder consumes the structured QueryParams and turns them into GORM
* clauses plus response metadata. Its flow is:
*   1. ApplyQuery walks each FilterParam/Search/Sort entry and stitches WHERE/
*      ORDER BY fragments using the configured column mappings.
*   2. FetchWithPagination clones the prepared query, counts the total rows,
*      executes the limited page query, and calculates QueryMeta (page/limit/
*      offset/total/hasPrev/hasNext).
*   3. BuildPaginationLinks mirrors the original URL + query string to emit
*      self/next/prev/first/last HATEOAS links that clients can follow.
* The helpers encapsulate per-operator SQL (value filters, LIKE patterns, IN/
* NOT IN, null checks) so controllers only deal with high-level configs.
*
*/

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type QueryMeta struct {
	Page       int   `json:"page"`
	Limit      int   `json:"limit"`
	Offset     int   `json:"offset"`
	Total      int64 `json:"total"`
	TotalPages int   `json:"total_pages"`
	HasNext    bool  `json:"has_next"`
	HasPrev    bool  `json:"has_prev"`
}

type QueryLinks struct {
	Self  string  `json:"self"`
	Next  *string `json:"next"`
	Prev  *string `json:"prev"`
	First *string `json:"first"`
	Last  *string `json:"last"`
}

// ApplyQuery adds filtering, searching, and sorting clauses to the supplied query.
func ApplyQuery(db *gorm.DB, params QueryParams, cfg QueryConfig) *gorm.DB {
	if db == nil {
		return db
	}

	query := db

	for _, filter := range params.Filters {
		column, ok := cfg.FilterableFields[filter.Field]
		if !ok {
			continue
		}
		query = applyFilter(query, column, filter)
	}

	if params.Search != "" {
		query = applySearch(query, params, cfg)
	}

	for _, sort := range params.Sort {
		column, ok := cfg.SortableFields[sort.Field]
		if !ok {
			continue
		}
		dir := strings.ToUpper(sort.Direction)
		if dir != "DESC" {
			dir = "ASC"
		}
		query = query.Order(fmt.Sprintf("%s %s", column, dir))
	}

	return query
}

// FetchWithPagination executes the query, returning paginated results alongside metadata.
func FetchWithPagination(query *gorm.DB, params QueryParams, dest interface{}) (QueryMeta, error) {
	if query == nil {
		return QueryMeta{}, fmt.Errorf("query must not be nil")
	}

	limit := params.Pagination.Limit
	if limit <= 0 {
		limit = 20
	}

	offset := params.Pagination.Offset
	if offset < 0 {
		offset = 0
	}

	page := params.Pagination.Page
	if page <= 0 && limit > 0 {
		page = (offset / limit) + 1
	}
	if page <= 0 {
		page = 1
	}

	var total int64
	if err := query.Session(&gorm.Session{}).Count(&total).Error; err != nil {
		return QueryMeta{}, err
	}

	dataQuery := query.Session(&gorm.Session{}).
		Offset(offset).
		Limit(limit)

	if err := dataQuery.Find(dest).Error; err != nil {
		return QueryMeta{}, err
	}

	var totalPages int
	if limit > 0 && total > 0 {
		totalPages = int(math.Ceil(float64(total) / float64(limit)))
	}

	meta := QueryMeta{
		Page:       page,
		Limit:      limit,
		Offset:     offset,
		Total:      total,
		TotalPages: totalPages,
		HasPrev:    page > 1 && total > 0,
		HasNext:    totalPages > 0 && page < totalPages,
	}

	return meta, nil
}

// BuildPaginationLinks generates standard pagination links preserving the existing query string.
func BuildPaginationLinks(c *gin.Context, meta QueryMeta) QueryLinks {
	links := QueryLinks{
		Self: c.Request.URL.Path,
	}

	if meta.Limit <= 0 {
		return links
	}

	basePath := c.Request.URL.Path
	original := cloneValues(c.Request.URL.Query())

	if self := buildLink(basePath, original, meta.Page, meta.Limit); self != nil {
		links.Self = *self
	}

	if meta.TotalPages > 0 {
		links.First = buildLink(basePath, original, 1, meta.Limit)
		links.Last = buildLink(basePath, original, meta.TotalPages, meta.Limit)
	} else {
		links.First = buildLink(basePath, original, 1, meta.Limit)
		links.Last = buildLink(basePath, original, 1, meta.Limit)
	}

	if meta.HasPrev {
		links.Prev = buildLink(basePath, original, meta.Page-1, meta.Limit)
	}
	if meta.HasNext {
		links.Next = buildLink(basePath, original, meta.Page+1, meta.Limit)
	}

	return links
}

func applyFilter(query *gorm.DB, column string, filter FilterParam) *gorm.DB {
	switch filter.Operator {
	case "eq":
		return applyValueFilter(query, column, "=", filter.Values)
	case "ne":
		return applyValueFilter(query, column, "<>", filter.Values)
	case "gt":
		return applyValueFilter(query, column, ">", filter.Values)
	case "gte":
		return applyValueFilter(query, column, ">=", filter.Values)
	case "lt":
		return applyValueFilter(query, column, "<", filter.Values)
	case "lte":
		return applyValueFilter(query, column, "<=", filter.Values)
	case "contains":
		return applyLikeFilter(query, column, "%", "%", filter.Values)
	case "starts":
		return applyLikeFilter(query, column, "", "%", filter.Values)
	case "ends":
		return applyLikeFilter(query, column, "%", "", filter.Values)
	case "in":
		if len(filter.Values) == 0 {
			return query
		}
		return query.Where(fmt.Sprintf("%s IN ?", column), filter.Values)
	case "nin":
		if len(filter.Values) == 0 {
			return query
		}
		return query.Where(fmt.Sprintf("%s NOT IN ?", column), filter.Values)
	case "null":
		return query.Where(fmt.Sprintf("%s IS NULL", column))
	case "notnull":
		return query.Where(fmt.Sprintf("%s IS NOT NULL", column))
	default:
		return query
	}
}

func applyValueFilter(query *gorm.DB, column, operator string, values []string) *gorm.DB {
	if len(values) == 0 {
		return query
	}
	return query.Where(fmt.Sprintf("%s %s ?", column, operator), values[0])
}

func applyLikeFilter(query *gorm.DB, column, prefix, suffix string, values []string) *gorm.DB {
	if len(values) == 0 {
		return query
	}
	pattern := fmt.Sprintf("%s%s%s", prefix, values[0], suffix)
	return query.Where(fmt.Sprintf("%s ILIKE ?", column), pattern)
}

func applySearch(query *gorm.DB, params QueryParams, cfg QueryConfig) *gorm.DB {
	if params.Search == "" || len(params.SearchFields) == 0 || len(cfg.SearchableFields) == 0 {
		return query
	}

	var clauses []string
	var args []interface{}

	for _, field := range params.SearchFields {
		column, ok := cfg.SearchableFields[field]
		if !ok {
			continue
		}
		clauses = append(clauses, fmt.Sprintf("%s ILIKE ?", column))
		args = append(args, "%"+params.Search+"%")
	}

	if len(clauses) == 0 {
		return query
	}

	return query.Where("(" + strings.Join(clauses, " OR ") + ")", args...)
}

func buildLink(path string, values url.Values, page, limit int) *string {
	if page <= 0 || limit <= 0 {
		return nil
	}
	clone := cloneValues(values)
	clone.Set("page", strconv.Itoa(page))
	clone.Set("limit", strconv.Itoa(limit))
	clone.Set("offset", strconv.Itoa((page-1)*limit))

	link := path
	if encoded := clone.Encode(); encoded != "" {
		link = fmt.Sprintf("%s?%s", path, encoded)
	}
	return &link
}

func cloneValues(values url.Values) url.Values {
	clone := url.Values{}
	for key, vals := range values {
		for _, v := range vals {
			clone.Add(key, v)
		}
	}
	return clone
}

