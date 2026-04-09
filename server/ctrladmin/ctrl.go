package ctrladmin

import "net/http"

func New(dbc any, proxy any) http.Handler {
	return http.NewServeMux()
}
