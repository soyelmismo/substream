package ctrladmin

import (
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	"log"
	"net/http"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/handlerutil"
	"go.senan.xyz/gonic/listenbrainz"
	"go.senan.xyz/gonic/tidalproxy"
)


func (c *Controller) ServeNotFound(_ *http.Request) *Response {
	return &Response{template: "not_found.tmpl", code: 404}
}

func (c *Controller) ServeLogin(_ *http.Request) *Response {
	return &Response{template: "login.tmpl"}
}

func (c *Controller) ServeHome(r *http.Request) *Response {
	user := r.Context().Value(CtxUser).(*db.User)

	data := &templateData{}
	// stats box
	var albums, artists, tracks int64

	data.Stats = StatsData{
		Albums:  albums,
		Artists: artists,
		Tracks:  tracks,
	}
	
	data.RequestRoot = handlerutil.BaseURL(r)
	data.DefaultListenBrainzURL = listenbrainz.BaseURL

	// users box
	allUsersQ := c.dbc.DB
	if !user.IsAdmin {
		allUsersQ = allUsersQ.Where("name=?", user.Name)
	}
	allUsersQ.Find(&data.AllUsers)

	return &Response{
		template: "home.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeUnlinkLastFMDo(r *http.Request) *Response {
	user := r.Context().Value(CtxUser).(*db.User)
	user.LastfmSession = ""
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}
	return &Response{redirect: "/admin/home"}
}

func (c *Controller) ServeLinkListenBrainzDo(r *http.Request) *Response {
	token := r.FormValue("token")
	url := r.FormValue("url")
	if token == "" || url == "" {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{"please provide a url and token"},
		}
	}
	user := r.Context().Value(CtxUser).(*db.User)
	user.ListenbrainzUrl = url
	user.ListenbrainzToken = token
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}
	return &Response{redirect: "/admin/home"}
}

func (c *Controller) ServeUnlinkListenBrainzDo(r *http.Request) *Response {
	user := r.Context().Value(CtxUser).(*db.User)
	user.ListenbrainzUrl = ""
	user.ListenbrainzToken = ""
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}
	return &Response{redirect: "/admin/home"}
}




func (c *Controller) ServeChangeUsername(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "change_username.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeChangeUsernameDo(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	usernameNew := r.FormValue("username")
	if err := validateUsername(usernameNew); err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{err.Error()},
		}
	}
	user.Name = usernameNew
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save username: %v", err)}}
	}
	return &Response{redirect: "/admin/home"}
}

func (c *Controller) ServeChangePassword(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "change_password.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeChangePasswordDo(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	passwordOne := r.FormValue("password_one")
	passwordTwo := r.FormValue("password_two")
	if err := validatePasswords(passwordOne, passwordTwo); err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{err.Error()},
		}
	}
	user.Password = passwordOne
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}
	return &Response{redirect: "/admin/home"}
}

func (c *Controller) ServeChangeAvatar(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "change_avatar.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeChangeAvatarDo(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	avatar, err := getAvatarFile(r)
	if err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{err.Error()},
		}
	}
	user.Avatar = avatar
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}
	return &Response{
		redirect: r.Referer(),
		flashN:   []string{"avatar saved successfully"},
	}
}

func (c *Controller) ServeDeleteAvatarDo(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	user.Avatar = nil
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}
	return &Response{
		redirect: r.Referer(),
		flashN:   []string{"avatar deleted successfully"},
	}
}

func (c *Controller) ServeDeleteUser(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "delete_user.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeDeleteUserDo(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	if user.IsAdmin {
		return &Response{
			redirect: "/admin/home",
			flashW:   []string{"can't delete the admin user"},
		}
	}
	if err := c.dbc.Delete(user).Error; err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("delete user: %v", err)}}
	}
	return &Response{redirect: "/admin/home"}
}

func (c *Controller) ServeCreateUser(_ *http.Request) *Response {
	return &Response{template: "create_user.tmpl"}
}

func (c *Controller) ServeCreateUserDo(r *http.Request) *Response {
	username := r.FormValue("username")
	if err := validateUsername(username); err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{err.Error()},
		}
	}
	passwordOne := r.FormValue("password_one")
	passwordTwo := r.FormValue("password_two")
	if err := validatePasswords(passwordOne, passwordTwo); err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{err.Error()},
		}
	}
	
	// Check role selection
	roleVal := r.FormValue("role")
	log.Printf("[ADMIN] Creating user %s, role: %q", username, roleVal)
	isAdmin := roleVal == "admin"
	
	user := db.User{
		Name:     username,
		Password: passwordOne,
		IsAdmin:  isAdmin,
	}
	if err := c.dbc.Create(&user).Error; err != nil {
		return &Response{
			redirect: r.Referer(),
			flashW:   []string{fmt.Sprintf("could not create user %q: %v", username, err)},
		}
	}
	
	role := "user"
	if isAdmin {
		role = "admin"
	}
	return &Response{
		redirect: "/admin/home",
		flashN:   []string{fmt.Sprintf("created %s %q successfully", role, username)},
	}
}

func getAvatarFile(r *http.Request) ([]byte, error) {
	err := r.ParseMultipartForm(10 << 20) // keep up to 10MB in memory
	if err != nil {
		return nil, err
	}
	file, _, err := r.FormFile("avatar")
	if err != nil {
		return nil, fmt.Errorf("read form file: %w", err)
	}
	i, _, err := image.Decode(file)
	if err != nil {
		return nil, fmt.Errorf("decode image: %w", err)
	}
	
	// Just stub out resizing since nfnt/resize is removed in SubStream
	var buff bytes.Buffer
	if err := jpeg.Encode(&buff, i, nil); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func selectedUserIfAdmin(c *Controller, r *http.Request) (*db.User, error) {
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername == "" {
		return nil, fmt.Errorf("please provide a username")
	}
	user := r.Context().Value(CtxUser).(*db.User)
	if !user.IsAdmin && user.Name != selectedUsername {
		return nil, fmt.Errorf("must be admin to perform actions for other users")
	}
	selectedUser := c.dbc.GetUserByName(selectedUsername)
	return selectedUser, nil
}
func (c *Controller) ServeProxies(r *http.Request) *Response {
	proxies, err := c.dbc.GetProxies()
	if err != nil {
		return &Response{code: 500, err: fmt.Sprintf("get proxies: %v", err)}
	}

	data := &templateData{}
	data.Proxies = proxies
	
	// Get mirror stats from MirrorManager
	if cachedProxy, ok := c.proxy.(*tidalproxy.CachedProxy); ok {
		if mgr := cachedProxy.GetMirrorManager(); mgr != nil {
			data.MirrorStats = mgr.GetStatus()
		}
	}
	
	return &Response{
		template: "proxies.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeAddProxyDo(r *http.Request) *Response {
	url := r.FormValue("url")
	name := r.FormValue("name")
	if url == "" {
		return &Response{redirect: r.Referer(), flashW: []string{"URL is required"}}
	}

	if err := c.dbc.AddProxy(url, name, "manual"); err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("add proxy: %v", err)}}
	}

	// update live pool
	if err := c.syncProxyPool(); err != nil {
		log.Printf("proxy: sync pool error after add: %v", err)
	}

	return &Response{redirect: "/admin/proxies", flashN: []string{"proxy added"}}
}

func (c *Controller) ServeDeleteProxyDo(r *http.Request) *Response {
	idString := r.FormValue("id")
	var idInt int
	fmt.Sscanf(idString, "%d", &idInt)

	if err := c.dbc.DeleteProxy(idInt); err != nil {
		return &Response{redirect: r.Referer(), flashW: []string{fmt.Sprintf("delete proxy: %v", err)}}
	}

	// update live pool
	if err := c.syncProxyPool(); err != nil {
		log.Printf("proxy: sync pool error after delete: %v", err)
	}

	return &Response{redirect: "/admin/proxies", flashN: []string{"proxy deleted"}}
}

func (c *Controller) syncProxyPool() error {
	proxies, err := c.dbc.GetProxies()
	if err != nil {
		return err
	}
	urls := make([]string, len(proxies))
	for i, p := range proxies {
		urls[i] = p.URL
	}
	c.proxy.SetInstances(urls)
	return nil
}
