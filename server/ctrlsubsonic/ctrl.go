package ctrlsubsonic

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/handlerutil"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/scrobble"
)

type CtxKey int

const (
	CtxUser CtxKey = iota
	CtxSession
	CtxParams
)

type MusicPath struct {
	Alias, Path string
}

func MusicPaths(paths []MusicPath) []string {
	var r []string
	for _, p := range paths {
		r = append(r, p.Path)
	}
	return r
}

type ProxyPathResolver func(in string) string

type Controller struct {
	*http.ServeMux

	dbc             *db.DB
	musicPaths      []MusicPath
	cacheCoverPath  string
	scrobblers      []scrobble.Scrobbler
}

func New(dbc *db.DB, musicPaths []MusicPath, scrobblers []scrobble.Scrobbler, cacheCoverPath string) *Controller {
	c := &Controller{
		ServeMux:   http.NewServeMux(),
		dbc:        dbc,
		musicPaths: musicPaths,
		scrobblers: scrobblers,
		cacheCoverPath:  cacheCoverPath,
	}

	chain := handlerutil.Chain(
		withParams,
		withRequiredParams,
		withUser(dbc),
	)
	chainRaw := handlerutil.Chain(
		chain,
		slow,
	)

	// Admin and system
	c.Handle("/ping", chain(resp(c.ServePing)))
	c.Handle("/getLicense", chain(resp(c.ServeGetLicence)))
	c.Handle("/getUser", chain(resp(c.ServeGetUser)))

	// Stubbed Routes
	c.Handle("/getPlayQueue", chain(resp(c.ServeGetPlayQueue)))
	c.Handle("/savePlayQueue", chain(resp(c.ServeSavePlayQueue)))

	// Raw
	c.Handle("/getCoverArt", chainRaw(respRaw(c.ServeGetCoverArt)))
	c.Handle("/stream", chainRaw(respRaw(c.ServeStream)))

	c.Handle("/", chain(resp(c.ServeNotFound)))

	return c
}

type (
	handlerSubsonic    func(r *http.Request) *spec.Response
	handlerSubsonicRaw func(w http.ResponseWriter, r *http.Request) *spec.Response
)

func resp(h handlerSubsonic) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := writeResp(w, r, h(r)); err != nil {
			log.Printf("error writing subsonic response: %v\n", err)
		}
	})
}

func respRaw(h handlerSubsonicRaw) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := writeResp(w, r, h(w, r)); err != nil {
			log.Printf("error writing raw subsonic response: %v\n", err)
		}
	})
}

func withParams(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := params.New(r)
		withParams := context.WithValue(r.Context(), CtxParams, params)
		next.ServeHTTP(w, r.WithContext(withParams))
	})
}

func withRequiredParams(next http.Handler) http.Handler {
	requiredParameters := []string{
		"u", "c",
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := r.Context().Value(CtxParams).(params.Params)
		for _, req := range requiredParameters {
			if _, err := params.Get(req); err != nil {
				_ = writeResp(w, r, spec.NewError(10, "please provide a %q parameter", req))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func withUser(dbc *db.DB) handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			params := r.Context().Value(CtxParams).(params.Params)
			username, _ := params.Get("u")
			password, _ := params.Get("p")
			token, _ := params.Get("t")
			salt, _ := params.Get("s")

			passwordAuth := token == "" && salt == ""
			tokenAuth := password == ""
			if tokenAuth == passwordAuth {
				_ = writeResp(w, r, spec.NewError(10, "please provide auth"))
				return
			}
			user := dbc.GetUserByName(username)
			if user == nil {
				_ = writeResp(w, r, spec.NewError(40, "invalid username"))
				return
			}
			var credsOk bool
			if tokenAuth {
				credsOk = checkCredsToken(user.Password, token, salt)
			} else {
				credsOk = checkCredsBasic(user.Password, password)
			}
			if !credsOk {
				_ = writeResp(w, r, spec.NewError(40, "invalid password"))
				return
			}
			withUser := context.WithValue(r.Context(), CtxUser, user)
			next.ServeHTTP(w, r.WithContext(withUser))
		})
	}
}

func slow(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w) 
		_ = rc.SetWriteDeadline(time.Time{}) 
		_ = rc.SetReadDeadline(time.Time{})  
		next.ServeHTTP(w, r)
	})
}

func checkCredsToken(password, token, salt string) bool {
	toHash := fmt.Sprintf("%s%s", password, salt)
	hash := md5.Sum([]byte(toHash))
	expToken := hex.EncodeToString(hash[:])
	return token == expToken
}

func checkCredsBasic(password, given string) bool {
	if len(given) >= 4 && given[:4] == "enc:" {
		bytes, _ := hex.DecodeString(given[4:])
		given = string(bytes)
	}
	return password == given
}

type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) write(buf []byte) {
	if ew.err != nil {
		return
	}
	_, ew.err = ew.w.Write(buf)
}

func writeResp(w http.ResponseWriter, r *http.Request, resp *spec.Response) error {
	if resp == nil {
		return nil
	}

	var res struct {
		XMLName        xml.Name `xml:"subsonic-response" json:"-"`
		*spec.Response `json:"subsonic-response"`
	}
	res.Response = resp

	params := r.Context().Value(CtxParams).(params.Params)

	ew := &errWriter{w: w}
	switch v, _ := params.Get("f"); v {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		data, err := json.Marshal(res)
		if err != nil {
			return fmt.Errorf("marshal to json: %w", err)
		}
		ew.write(data)

	case "jsonp":
		w.Header().Set("Content-Type", "application/javascript")
		data, err := json.Marshal(res)
		if err != nil {
			return fmt.Errorf("marshal to jsonp: %w", err)
		}
		pCall := params.GetOr("callback", "cb")
		ew.write([]byte(pCall))
		ew.write([]byte("("))
		ew.write(data)
		ew.write([]byte(");"))

	default:
		w.Header().Set("Content-Type", "application/xml")
		data, err := xml.MarshalIndent(res, "", "    ")
		if err != nil {
			return fmt.Errorf("marshal to xml: %w", err)
		}

		ew.write(data)
	}

	return ew.err
}
