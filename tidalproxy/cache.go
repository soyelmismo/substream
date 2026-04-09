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
	tracks   sync.Map
	albums   sync.Map
	artists  sync.Map
	albumArt sync.Map // int -> string UUID
	ttl      time.Duration
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
