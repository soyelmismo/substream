package ctrladmin

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/sessions"

	"go.senan.xyz/gonic/db"
)

func (c *Controller) ServeLoginDo(w http.ResponseWriter, r *http.Request) {
	session := r.Context().Value(CtxSession).(*sessions.Session)
	username := r.FormValue("username")
	password := r.FormValue("password")
	if username == "" || password == "" {
		sessAddFlashW(session, []string{"please provide username and password"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, r.Referer(), http.StatusSeeOther)
		return
	}
	user := c.dbc.GetUserByName(username)
	
	// If user doesn't exist, check if auto-register is enabled
	if user == nil {
		autoRegister := c.dbc.GetSetting("auto_register", "false")
		if autoRegister == "true" {
			// Create new user automatically
			newUser := db.User{
				Name:     username,
				Password: password,
				IsAdmin:  false,
			}
			if err := c.dbc.Create(&newUser).Error; err != nil {
				log.Printf("[LOGIN] Auto-register failed for user %s: %v", username, err)
				sessAddFlashW(session, []string{"could not auto-register user"})
				sessLogSave(session, w, r)
				http.Redirect(w, r, r.Referer(), http.StatusSeeOther)
				return
			}
			log.Printf("[LOGIN] Auto-registered new user: %s", username)
			user = &newUser
		} else {
			sessAddFlashW(session, []string{"invalid username / password"})
			sessLogSave(session, w, r)
			http.Redirect(w, r, r.Referer(), http.StatusSeeOther)
			return
		}
	}
	
	// Check password for existing user
	if password != user.Password {
		sessAddFlashW(session, []string{"invalid username / password"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, r.Referer(), http.StatusSeeOther)
		return
	}
	
	// put the user name into the session. future endpoints after this one
	// are wrapped with WithUserSession() which will get the name from the
	// session and put the row into the request context
	session.Values["user"] = user.ID
	sessLogSave(session, w, r)
	http.Redirect(w, r, c.resolveProxyPath("/admin/home"), http.StatusSeeOther)
}

func (c *Controller) ServeLogout(w http.ResponseWriter, r *http.Request) {
	session := r.Context().Value(CtxSession).(*sessions.Session)
	session.Options.MaxAge = -1
	sessLogSave(session, w, r)
	http.Redirect(w, r, c.resolveProxyPath("/admin/login"), http.StatusSeeOther)
}

func (c *Controller) ServeSettings(r *http.Request) *Response {
	autoRegister := c.dbc.GetSetting("auto_register", "false")
	proxyStreams := c.dbc.GetSetting("proxy_streams", "false")
	return &Response{
		template: "settings.tmpl",
		data: &templateData{
			AutoRegister: autoRegister == "true",
			ProxyStreams: proxyStreams == "true",
		},
	}
}

func (c *Controller) ServeSettingsDo(r *http.Request) *Response {
	autoRegister := r.FormValue("auto_register")
	if autoRegister == "" {
		autoRegister = "false"
	}
	
	proxyStreams := r.FormValue("proxy_streams")
	if proxyStreams == "" {
		proxyStreams = "false"
	}
	
	if err := c.dbc.SetSetting("auto_register", autoRegister); err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{fmt.Sprintf("could not save auto_register setting: %v", err)},
		}
	}
	
	if err := c.dbc.SetSetting("proxy_streams", proxyStreams); err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{fmt.Sprintf("could not save proxy_streams setting: %v", err)},
		}
	}
	
	status := "disabled"
	if autoRegister == "true" {
		status = "enabled"
	}
	
	proxyStatus := "disabled"
	if proxyStreams == "true" {
		proxyStatus = "enabled"
	}
	
	return &Response{
		redirect: "/admin/home",
		flashN:   []string{fmt.Sprintf("auto-register %s, proxy streams %s", status, proxyStatus)},
	}
}
