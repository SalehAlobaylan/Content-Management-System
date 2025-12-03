package utils

/*
*
* Query Parser orchestrates every incoming query string into a predictable
* QueryParams struct. It normalizes pagination (page/limit/offset), aligns
* multi-field sorts with the resource config, validates filter operators and
* their values, and constrains free-form search to the configured fields. The
* flow is:
*   1. derive pagination defaults/maxes from QueryConfig and clamp page/limit/offset,
*   2. parse comma-delimited sort/order sequences and map each to sortable columns,
*   3. sanitize search text/fields so only whitelisted columns participate,
*   4. iterate remaining query keys, interpret operator prefixes (eq/gt/in/etc),
*      and emit structured FilterParams,
*   5. return QueryParams so downstream builders can apply GORM clauses safely.
*
 */

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

var supportedFilterOperators = map[string]struct{}{
	"eq":       {},
	"ne":       {},
	"gt":       {},
	"gte":      {},
	"lt":       {},
	"lte":      {},
	"in":       {},
	"nin":      {},
	"contains": {},
	"starts":   {},
	"ends":     {},
	"null":     {},
	"notnull":  {},
}

// QueryConfig describes how query params should be parsed for a resource.
type QueryConfig struct {
	DefaultLimit          int
	MaxLimit              int
	DefaultSort           []SortParam
	SortableFields        map[string]string
	FilterableFields      map[string]string
	SearchableFields      map[string]string
	DefaultSearchFields   []string
	FieldDefaultOperators map[string]string
}

type PaginationParams struct {
	Page   int
	Limit  int
	Offset int
}

type SortParam struct {
	Field     string
	Direction string
}

type FilterParam struct {
	Field    string
	Operator string
	Values   []string
}

type QueryParams struct {
	Pagination   PaginationParams
	Sort         []SortParam
	Filters      []FilterParam
	Search       string
	SearchFields []string
}

// ParseQueryParams converts incoming query parameters into structured QueryParams
// based on the provided configuration.
func ParseQueryParams(c *gin.Context, cfg QueryConfig) (QueryParams, error) {
	maxLimit := cfg.MaxLimit
	if maxLimit <= 0 {
		maxLimit = 100
	}

	defaultLimit := cfg.DefaultLimit
	if defaultLimit <= 0 || defaultLimit > maxLimit {
		defaultLimit = minInt(20, maxLimit)
	}

	params := QueryParams{
		Pagination: PaginationParams{
			Page:  1,
			Limit: defaultLimit,
		},
	}

	if limitStr := strings.TrimSpace(c.Query("limit")); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			return QueryParams{}, fmt.Errorf("invalid limit parameter")
		}
		if limit > maxLimit {
			limit = maxLimit
		}
		params.Pagination.Limit = limit
	}

	if pageStr := strings.TrimSpace(c.Query("page")); pageStr != "" {
		page, err := strconv.Atoi(pageStr)
		if err != nil || page <= 0 {
			return QueryParams{}, fmt.Errorf("invalid page parameter")
		}
		params.Pagination.Page = page
	}

	offsetProvided := false
	if offsetStr := strings.TrimSpace(c.Query("offset")); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			return QueryParams{}, fmt.Errorf("invalid offset parameter")
		}
		params.Pagination.Offset = offset
		offsetProvided = true
	}

	if offsetProvided {
		params.Pagination.Page = (params.Pagination.Offset / params.Pagination.Limit) + 1
	} else {
		params.Pagination.Offset = (params.Pagination.Page - 1) * params.Pagination.Limit
	}

	params.Sort = parseSortParams(c, cfg)
	params.SearchFields = defaultSearchFields(cfg)

	if len(params.Sort) == 0 && len(cfg.DefaultSort) > 0 {
		params.Sort = append(params.Sort, cfg.DefaultSort...)
	}

	params.Search = strings.TrimSpace(c.Query("search"))
	if sf := strings.TrimSpace(c.Query("search_fields")); sf != "" {
		requested := sanitizeList(sf)
		valid := filterSearchable(requested, cfg)
		if len(valid) > 0 {
			params.SearchFields = valid
		}
	}

	if len(params.SearchFields) == 0 {
		params.SearchFields = defaultSearchFields(cfg)
	}

	filters, err := parseFilters(c, cfg)
	if err != nil {
		return QueryParams{}, err
	}
	params.Filters = filters

	return params, nil
}

func parseSortParams(c *gin.Context, cfg QueryConfig) []SortParam {
	sortFields := sanitizeList(c.Query("sort"))
	orderValues := sanitizeList(c.Query("order"))

	var sorts []SortParam
	for idx, field := range sortFields {
		if field == "" {
			continue
		}
		if _, ok := cfg.SortableFields[field]; !ok {
			continue
		}
		direction := "asc"
		if idx < len(orderValues) {
			dir := strings.ToLower(orderValues[idx])
			if dir == "desc" {
				direction = "desc"
			}
		}
		sorts = append(sorts, SortParam{
			Field:     field,
			Direction: direction,
		})
	}
	return sorts
}

func parseFilters(c *gin.Context, cfg QueryConfig) ([]FilterParam, error) {
	values := c.Request.URL.Query()
	reserved := map[string]struct{}{
		"page":          {},
		"limit":         {},
		"offset":        {},
		"sort":          {},
		"order":         {},
		"search":        {},
		"search_fields": {},
	}

	var filters []FilterParam
	for key, vals := range values {
		if _, skip := reserved[key]; skip {
			continue
		}
		if _, allowed := cfg.FilterableFields[key]; !allowed {
			continue
		}
		defaultOp := cfg.defaultOperator(key)
		for _, raw := range vals {
			op, parsedValues, err := parseFilterValue(strings.TrimSpace(raw), defaultOp)
			if err != nil {
				return nil, fmt.Errorf("invalid filter for '%s': %w", key, err)
			}
			filters = append(filters, FilterParam{
				Field:    key,
				Operator: op,
				Values:   parsedValues,
			})
		}
	}
	return filters, nil
}

func (cfg QueryConfig) defaultOperator(field string) string {
	if cfg.FieldDefaultOperators == nil {
		return "eq"
	}
	if op, ok := cfg.FieldDefaultOperators[field]; ok {
		return op
	}
	return "eq"
}

func parseFilterValue(raw, defaultOp string) (string, []string, error) {
	if raw == "" && defaultOp != "null" && defaultOp != "notnull" {
		return "", nil, fmt.Errorf("empty value")
	}

	op := defaultOp
	value := raw

	if parts := strings.SplitN(raw, ":", 2); len(parts) == 2 {
		candidate := strings.ToLower(strings.TrimSpace(parts[0]))
		if _, ok := supportedFilterOperators[candidate]; ok {
			op = candidate
			value = parts[1]
		}
	} else {
		lowered := strings.ToLower(raw)
		if _, ok := supportedFilterOperators[lowered]; ok && (lowered == "null" || lowered == "notnull") {
			op = lowered
			value = ""
		}
	}

	switch op {
	case "null", "notnull":
		return op, nil, nil
	case "in", "nin":
		if strings.TrimSpace(value) == "" {
			return "", nil, fmt.Errorf("operator '%s' requires at least one value", op)
		}
		parts := sanitizeList(value)
		if len(parts) == 0 {
			return "", nil, fmt.Errorf("operator '%s' requires at least one value", op)
		}
		return op, parts, nil
	default:
		val := strings.TrimSpace(value)
		if val == "" {
			return "", nil, fmt.Errorf("empty value")
		}
		return op, []string{val}, nil
	}
}

func sanitizeList(input string) []string {
	if input == "" {
		return nil
	}
	raw := strings.Split(input, ",")
	var cleaned []string
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item != "" {
			cleaned = append(cleaned, item)
		}
	}
	return cleaned
}

func defaultSearchFields(cfg QueryConfig) []string {
	if len(cfg.DefaultSearchFields) > 0 {
		var filtered []string
		for _, field := range cfg.DefaultSearchFields {
			if cfg.SearchableFields == nil {
				continue
			}
			if _, ok := cfg.SearchableFields[field]; ok {
				filtered = append(filtered, field)
			}
		}
		return append([]string{}, filtered...)
	}
	if len(cfg.SearchableFields) == 0 {
		return nil
	}
	var fields []string
	for field := range cfg.SearchableFields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func filterSearchable(fields []string, cfg QueryConfig) []string {
	if len(fields) == 0 {
		return nil
	}
	var filtered []string
	for _, field := range fields {
		if _, ok := cfg.SearchableFields[field]; ok {
			filtered = append(filtered, field)
		}
	}
	return filtered
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
