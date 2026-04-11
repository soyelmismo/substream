package tidalproxy

import (
	"context"
	"strconv"
	"time"

	"go.senan.xyz/gonic/internal/cache"
)

// CachedProxy wraps TidalProxy with type-safe LRU+TTL caches
type CachedProxy struct {
	TidalProxy
	tracks     *cache.Cache[*TidalTrack]
	albums     *cache.Cache[*TidalAlbum]
	artists    *cache.Cache[*TidalArtistDetail]
	albumArt   *cache.Cache[string] // int -> string UUID
	albumCount *cache.Cache[int]    // artistID -> int (album count)
}

func NewCachedProxy(base TidalProxy, ttl time.Duration) *CachedProxy {
	return &CachedProxy{
		TidalProxy: base,
		tracks: cache.New[*TidalTrack](cache.Config{
			Name:            "tidal-tracks",
			MaxSize:         500,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albums: cache.New[*TidalAlbum](cache.Config{
			Name:            "tidal-albums",
			MaxSize:         200,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artists: cache.New[*TidalArtistDetail](cache.Config{
			Name:            "tidal-artists",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albumArt: cache.New[string](cache.Config{
			Name:            "tidal-album-art",
			MaxSize:         1000,
			DefaultTTL:      ttl * 2, // Album art changes less frequently
			CleanupInterval: 10 * time.Minute,
		}),
		albumCount: cache.New[int](cache.Config{
			Name:            "tidal-album-count",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
	}
}

func (c *CachedProxy) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	key := strconv.Itoa(trackID)
	if cached := c.tracks.Get(key); cached != nil {
		return cached, nil
	}
	t, err := c.TidalProxy.GetTrackInfo(ctx, trackID)
	if err == nil && t != nil {
		c.tracks.Set(key, t, 0)
		if t.Album.Cover != "" {
			c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
		}
	}
	return t, err
}

func (c *CachedProxy) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	key := strconv.Itoa(albumID)
	if cached := c.albums.Get(key); cached != nil {
		return cached, nil
	}
	t, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
	if err == nil && t != nil {
		c.albums.Set(key, t, 0)
		if t.Cover != "" {
			c.albumArt.Set(key, t.Cover, 0)
		}
	}
	return t, err
}

func (c *CachedProxy) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	key := strconv.Itoa(artistID)
	if cached := c.artists.Get(key); cached != nil {
		return cached, nil
	}
	t, err := c.TidalProxy.GetArtistInfo(ctx, artistID)
	if err == nil && t != nil {
		c.artists.Set(key, t, 0)
	}
	return t, err
}

// GetMirrorManager returns the underlying MirrorManager if the base is a Pool
func (c *CachedProxy) GetMirrorManager() *MirrorManager {
	if pool, ok := c.TidalProxy.(*Pool); ok {
		return pool.GetMirrorManager()
	}
	return nil
}

func (c *CachedProxy) GetCoverUUIDForAlbum(ctx context.Context, albumID int) string {
	key := strconv.Itoa(albumID)
	if cached := c.albumArt.Get(key); cached != "" {
		return cached
	}
	a, err := c.GetAlbumInfo(ctx, albumID)
	if err == nil && a != nil && a.Cover != "" {
		return a.Cover
	}
	return ""
}

func (c *CachedProxy) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	albums, err := c.TidalProxy.SearchAlbums(ctx, query, limit, offset)
	if err == nil {
		for _, a := range albums {
			if a.Cover != "" {
				c.albumArt.Set(strconv.Itoa(a.ID), a.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error) {
	page, err := c.TidalProxy.GetArtistAlbums(ctx, artistID, skipTracks)
	if err == nil && page != nil {
		if len(page.Albums.Items) > 0 {
			for _, a := range page.Albums.Items {
				if a.Cover != "" {
					c.albumArt.Set(strconv.Itoa(a.ID), a.Cover, 0)
				}
			}
		}
		for _, t := range page.Tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

// GetArtistAlbumCount returns cached album count for an artist, fetching if needed
func (c *CachedProxy) GetArtistAlbumCount(ctx context.Context, artistID int) int {
	key := strconv.Itoa(artistID)
	if cached := c.albumCount.Get(key); cached != 0 {
		return cached
	}

	// Fetch from API
	page, err := c.GetArtistAlbums(ctx, artistID, true)
	count := 0
	if err == nil && page != nil {
		count = len(page.Albums.Items)
	}

	// Store in cache
	c.albumCount.Set(key, count, 0)
	return count
}

// Close stops all cache cleanup goroutines
func (c *CachedProxy) Close() {
	c.tracks.Stop()
	c.albums.Stop()
	c.artists.Stop()
	c.albumArt.Stop()
	c.albumCount.Stop()
}
