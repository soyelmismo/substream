// package params provides methods on url.Values for parsing params for the subsonic api
package params

import (
	"errors"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

var ErrNoValues = errors.New("no values provided")

type Params url.Values

func New(r *http.Request) Params {
	params := r.URL.Query()
	if err := r.ParseForm(); err == nil {
		maps.Copy(params, r.Form)
	}
	return Params(params)
}

func (p Params) get(key string) []string {
	return p[key]
}

func (p Params) getFirst(keys []string) []string {
	for _, k := range keys {
		if v, ok := p[k]; ok {
			return v
		}
	}
	return nil
}

// Generic helper for single value parsing
func getSingle[T any](p Params, keys []string, or *T) (T, error) {
	var vals []string
	if len(keys) == 1 {
		vals = p.get(keys[0])
	} else {
		vals = p.getFirst(keys)
	}

	if len(vals) == 0 {
		if or != nil {
			return *or, nil
		}
		var zero T
		return zero, ErrNoValues
	}

	var ret T
	err := parse(vals, &ret)
	return ret, err
}

// Generic helper for list parsing
func getList[T any](p Params, keys []string, or []T) ([]T, error) {
	var vals []string
	if len(keys) == 1 {
		vals = p.get(keys[0])
	} else {
		vals = p.getFirst(keys)
	}

	if len(vals) == 0 {
		if or != nil {
			return or, nil
		}
		return nil, ErrNoValues
	}

	var ret []T
	err := parse(vals, &ret)
	return ret, err
}

// String methods
func (p Params) Get(key string) (string, error)          { return getSingle[string](p, []string{key}, nil) }
func (p Params) GetFirst(keys ...string) (string, error) { return getSingle[string](p, keys, nil) }
func (p Params) GetOr(key string, or string) string      { v, _ := getSingle(p, []string{key}, &or); return v }
func (p Params) GetFirstOr(or string, keys ...string) string {
	v, _ := getSingle(p, keys, &or)
	return v
}

// Int methods
func (p Params) GetInt(key string) (int, error)          { return getSingle[int](p, []string{key}, nil) }
func (p Params) GetFirstInt(keys ...string) (int, error) { return getSingle[int](p, keys, nil) }
func (p Params) GetOrInt(key string, or int) int         { v, _ := getSingle(p, []string{key}, &or); return v }
func (p Params) GetFirstOrInt(or int, keys ...string) int {
	v, _ := getSingle(p, keys, &or)
	return v
}

// Float methods
func (p Params) GetFloat(key string) (float64, error)          { return getSingle[float64](p, []string{key}, nil) }
func (p Params) GetFirstFloat(keys ...string) (float64, error) { return getSingle[float64](p, keys, nil) }
func (p Params) GetOrFloat(key string, or float64) float64     { v, _ := getSingle(p, []string{key}, &or); return v }
func (p Params) GetFirstOrFloat(or float64, keys ...string) float64 {
	v, _ := getSingle(p, keys, &or)
	return v
}

// ID methods
func (p Params) GetID(key string) (specid.ID, error)          { return getSingle[specid.ID](p, []string{key}, nil) }
func (p Params) GetFirstID(keys ...string) (specid.ID, error) { return getSingle[specid.ID](p, keys, nil) }
func (p Params) GetOrID(key string, or specid.ID) specid.ID   { v, _ := getSingle(p, []string{key}, &or); return v }
func (p Params) GetFirstOrID(or specid.ID, keys ...string) specid.ID {
	v, _ := getSingle(p, keys, &or)
	return v
}

// Bool methods
func (p Params) GetBool(key string) (bool, error)          { return getSingle[bool](p, []string{key}, nil) }
func (p Params) GetFirstBool(keys ...string) (bool, error) { return getSingle[bool](p, keys, nil) }
func (p Params) GetOrBool(key string, or bool) bool        { v, _ := getSingle(p, []string{key}, &or); return v }
func (p Params) GetFirstOrBool(or bool, keys ...string) bool {
	v, _ := getSingle(p, keys, &or)
	return v
}

// Time methods
func (p Params) GetTime(key string) (time.Time, error)          { return getSingle[time.Time](p, []string{key}, nil) }
func (p Params) GetFirstTime(keys ...string) (time.Time, error) { return getSingle[time.Time](p, keys, nil) }
func (p Params) GetOrTime(key string, or time.Time) time.Time   { v, _ := getSingle(p, []string{key}, &or); return v }
func (p Params) GetFirstOrTime(or time.Time, keys ...string) time.Time {
	v, _ := getSingle(p, keys, &or)
	return v
}

// List methods (examples)
func (p Params) GetList(key string) ([]string, error)          { return getList[string](p, []string{key}, nil) }
func (p Params) GetIntList(key string) ([]int, error)          { return getList[int](p, []string{key}, nil) }
func (p Params) GetIDList(key string) ([]specid.ID, error)     { return getList[specid.ID](p, []string{key}, nil) }
func (p Params) GetOrIntList(key string, or []int) []int       { v, _ := getList(p, []string{key}, or); return v }

// Internal parsing logic
func parseStr(in string) (string, error)    { return in, nil }
func parseInt(in string) (int, error)       { return strconv.Atoi(in) }
func parseFloat(in string) (float64, error) { return strconv.ParseFloat(in, 64) }
func parseID(in string) (specid.ID, error)  { return specid.New(in) }
func parseBool(in string) (bool, error)     { return strconv.ParseBool(in) }

func parseTime(in string) (time.Time, error) {
	ms, err := strconv.Atoi(in)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, int64(ms)*1e6), nil
}

func parse(values []string, i any) error {
	if len(values) == 0 {
		return ErrNoValues
	}
	var err error
	switch v := i.(type) {
	case *string:
		*v, err = parseStr(values[0])
	case *int:
		*v, err = parseInt(values[0])
	case *float64:
		*v, err = parseFloat(values[0])
	case *specid.ID:
		*v, err = parseID(values[0])
	case *bool:
		*v, err = parseBool(values[0])
	case *time.Time:
		*v, err = parseTime(values[0])
	case *[]string:
		for _, val := range values {
			p, e := parseStr(val)
			if e != nil {
				return e
			}
			*v = append(*v, p)
		}
	case *[]int:
		for _, val := range values {
			p, e := parseInt(val)
			if e != nil {
				return e
			}
			*v = append(*v, p)
		}
	case *[]specid.ID:
		for _, val := range values {
			p, e := parseID(val)
			if e != nil {
				return e
			}
			*v = append(*v, p)
		}
	}
	return err
}
