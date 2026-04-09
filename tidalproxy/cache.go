package tidalproxy

import (
	"context"
	"sync"
	"time"
)

type cacheItem struct {
	data interface{}
	exp  time.Time
}

// CachedProxy wraps TidalProxy with an in-memory TTL cache
type CachedProxy struct {
	TidalProxy
	tracks     sync.Map
	albums     sync.Map
	artists    sync.Map
	albumArt   sync.Map // int -> string UUID
	albumCount sync.Map // artistID -> int (album count)
	ttl        time.Duration
}

func NewCachedProxy(base TidalProxy, ttl time.Duration) *CachedProxy {
	return &CachedProxy{
		TidalProxy: base,
		ttl:        ttl,
	}
}

func (c *CachedProxy) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	if v, ok := c.tracks.Load(trackID); ok {
		item := v.(cacheItem)
		if time.Now().Before(item.exp) {
			return item.data.(*TidalTrack), nil
		}
	}
	t, err := c.TidalProxy.GetTrackInfo(ctx, trackID)
	if err == nil {
		c.tracks.Store(trackID, cacheItem{data: t, exp: time.Now().Add(c.ttl)})
		if t.Album.Cover != "" {
			c.albumArt.Store(t.Album.ID, t.Album.Cover)
		}
	}
	return t, err
}

func (c *CachedProxy) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	if v, ok := c.albums.Load(albumID); ok {
		item := v.(cacheItem)
		if time.Now().Before(item.exp) {
			return item.data.(*TidalAlbum), nil
		}
	}
	t, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
	if err == nil {
		c.albums.Store(albumID, cacheItem{data: t, exp: time.Now().Add(c.ttl)})
		if t.Cover != "" {
			c.albumArt.Store(albumID, t.Cover)
		}
	}
	return t, err
}

func (c *CachedProxy) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	if v, ok := c.artists.Load(artistID); ok {
		item := v.(cacheItem)
		if time.Now().Before(item.exp) {
			return item.data.(*TidalArtistDetail), nil
		}
	}
	t, err := c.TidalProxy.GetArtistInfo(ctx, artistID)
	if err == nil {
		c.artists.Store(artistID, cacheItem{data: t, exp: time.Now().Add(c.ttl)})
	}
	return t, err
}

func (c *CachedProxy) GetCoverUUIDForAlbum(ctx context.Context, albumID int) string {
	if v, ok := c.albumArt.Load(albumID); ok {
		return v.(string)
	}
	a, err := c.GetAlbumInfo(ctx, albumID)
	if err == nil && a.Cover != "" {
		return a.Cover
	}
	return ""
}

func (c *CachedProxy) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	albums, err := c.TidalProxy.SearchAlbums(ctx, query, limit, offset)
	if err == nil {
		for _, a := range albums {
			if a.Cover != "" {
				c.albumArt.Store(a.ID, a.Cover)
			}
		}
	}
	return albums, err
}

func (c *CachedProxy) SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.SearchTracks(ctx, query, limit, offset)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Store(t.Album.ID, t.Album.Cover)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error) {
	page, err := c.TidalProxy.GetArtistAlbums(ctx, artistID, skipTracks)
	if err == nil {
		if len(page.Albums.Items) > 0 {
			for _, a := range page.Albums.Items {
				if a.Cover != "" {
					c.albumArt.Store(a.ID, a.Cover)
				}
			}
		}
		for _, t := range page.Tracks {
			if t.Album.Cover != "" {
				c.albumArt.Store(t.Album.ID, t.Album.Cover)
			}
		}
	}
	return page, err
}

func (c *CachedProxy) GetTopTracks(ctx context.Context, limit int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.GetTopTracks(ctx, limit)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Store(t.Album.ID, t.Album.Cover)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.GetRecommendations(ctx, trackID)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Store(t.Album.ID, t.Album.Cover)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.GetArtistTopTracks(ctx, artistID, limit)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Store(t.Album.ID, t.Album.Cover)
			}
		}
	}
	return tracks, err
}

// GetArtistAlbumCount returns cached album count for an artist, fetching if needed
func (c *CachedProxy) GetArtistAlbumCount(ctx context.Context, artistID int) int {
	// Check cache first
	if v, ok := c.albumCount.Load(artistID); ok {
		item := v.(cacheItem)
		if time.Now().Before(item.exp) {
			return item.data.(int)
		}
	}

	// Fetch from API
	page, err := c.GetArtistAlbums(ctx, artistID, true)
	count := 0
	if err == nil && page != nil {
		count = len(page.Albums.Items)
	}

	// Store in cache
	c.albumCount.Store(artistID, cacheItem{data: count, exp: time.Now().Add(c.ttl)})
	return count
}
