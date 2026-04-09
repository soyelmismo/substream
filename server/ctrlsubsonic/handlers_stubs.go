package ctrlsubsonic

import (
	"net/http"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
)

func (c *Controller) ServePing(_ *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeGetLicence(_ *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeGetUser(_ *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeGetPlayQueue(_ *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeSavePlayQueue(_ *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeNotFound(_ *http.Request) *spec.Response { return spec.NewResponse() }

func (c *Controller) ServeGetCoverArt(w http.ResponseWriter, r *http.Request) *spec.Response { return nil }
func (c *Controller) ServeStream(w http.ResponseWriter, r *http.Request) *spec.Response { return nil }
