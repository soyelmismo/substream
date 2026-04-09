package ctrlsubsonic

import (
	"net/http"

	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
)

func (c *Controller) ServeGetGenres(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.Genres = &spec.Genres{
		List: []*spec.Genre{},
	}
	return sub
}

func (c *Controller) ServeGetInternetRadioStations(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.InternetRadioStations = &spec.InternetRadioStations{
		List: []*spec.InternetRadioStation{},
	}
	return sub
}

func (c *Controller) ServeGetNewestPodcasts(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.NewestPodcasts = &spec.NewestPodcasts{
		List: []*spec.PodcastEpisode{},
	}
	return sub
}

func (c *Controller) ServeGetPodcasts(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.Podcasts = &spec.Podcasts{
		List: []*spec.PodcastChannel{},
	}
	return sub
}
