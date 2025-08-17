package duckdb

func asAny[T any](in []T) (out []any) {
	for _, v := range in {
		out = append(out, v)
	}
	return
}
