package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/shamaton/msgpack"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/sync/singleflight"
)

const (
	SQLDirectory              = "../sql/"
	AssignmentsDirectory      = "/home/isucon/webapp/assignments/"
	InitDataDirectory         = "../data/"
	SessionName               = "isucholar_go"
	mysqlErrNumDuplicateEntry = 1062
)

type handlers struct {
	DB *sqlx.DB
}

type UserSession struct {
	UserID   string
	UserName string
	IsAdmin  bool
	UserCode string
}

type UnreadAnnouncements struct {
	AnnouncementID string `db:"announcement_id"`
	UserID         string `db:"user_id"`
	IsDeleted      bool   `db:"is_deleted"`
}

// Encode / Decode
func EncodePtr(valuePtr interface{}) string {
	d, _ := msgpack.Encode(valuePtr)
	return string(d)
}
func DecodePtrStringCmd(input *redis.StringCmd, valuePtr interface{}) {
	msgpack.Decode([]byte(input.Val()), valuePtr)
}
func DecodePtrSliceCmdElem(partsOfSliceCmd interface{}, valuePtr interface{}) {
	msgpack.Decode([]byte(partsOfSliceCmd.(string)), valuePtr)
}

// unread_announcements
var rdb0 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   0, // 0 - 15
})

// course: key:ID / value:status
var rdb1 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   1, // 0 - 15
})

// class: key:ID / value:submission_closed
var rdb2 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   2, // 0 - 15
})

// submission: classID -> userID
var rdb3 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   3, // 0 - 15
})

// user: userCode -> userID
var rdb4 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   4, // 0 - 15
})

var sessionCache = sync.Map{}

func main() {
	go func() { log.Println(http.ListenAndServe(":9009", nil)) }()
	go syncGPA()
	e := echo.New()
	e.Debug = true
	e.Server.Addr = fmt.Sprintf(":%v", GetEnv("PORT", "7000"))
	e.HideBanner = true

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(session.Middleware(sessions.NewCookieStore([]byte("trapnomura"))))

	db, _ := GetDB(false)
	db.SetMaxOpenConns(300)
	db.SetMaxIdleConns(300)

	h := &handlers{
		DB: db,
	}

	e.POST("/initialize", h.Initialize)

	e.POST("/login", h.Login)
	e.POST("/logout", h.Logout)
	API := e.Group("/api", h.IsLoggedIn)
	{
		usersAPI := API.Group("/users")
		{
			usersAPI.GET("/me", h.GetMe)
			usersAPI.GET("/me/courses", h.GetRegisteredCourses)
			usersAPI.PUT("/me/courses", h.RegisterCourses)
			usersAPI.GET("/me/grades", h.GetGrades)
		}
		coursesAPI := API.Group("/courses")
		{
			coursesAPI.GET("", h.SearchCourses)
			coursesAPI.POST("", h.AddCourse, h.IsAdmin)
			coursesAPI.GET("/:courseID", h.GetCourseDetail)
			coursesAPI.PUT("/:courseID/status", h.SetCourseStatus, h.IsAdmin)
			coursesAPI.GET("/:courseID/classes", h.GetClasses)
			coursesAPI.POST("/:courseID/classes", h.AddClass, h.IsAdmin)
			coursesAPI.POST("/:courseID/classes/:classID/assignments", h.SubmitAssignment)
			coursesAPI.PUT("/:courseID/classes/:classID/assignments/scores", h.RegisterScores, h.IsAdmin)
			coursesAPI.GET("/:courseID/classes/:classID/assignments/export", h.DownloadSubmittedAssignments, h.IsAdmin)
		}
		announcementsAPI := API.Group("/announcements")
		{
			announcementsAPI.GET("", h.GetAnnouncementList)
			announcementsAPI.POST("", h.AddAnnouncement, h.IsAdmin)
			announcementsAPI.GET("/:announcementID", h.GetAnnouncementDetail)
		}
	}

	e.Logger.Error(e.StartServer(e.Server))
}

type InitializeResponse struct {
	Language string `json:"language"`
}

// Initialize POST /initialize ??????????????????????????????
func (h *handlers) Initialize(c echo.Context) error {
	dbForInit, _ := GetDB(true)

	files := []string{
		"1_schema.sql",
		"2_init.sql",
		"3_sample.sql",
	}
	for _, file := range files {
		data, err := os.ReadFile(SQLDirectory + file)
		if err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		if _, err := dbForInit.Exec(string(data)); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}

	if err := exec.Command("rm", "-rf", AssignmentsDirectory).Run(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if err := exec.Command("cp", "-r", InitDataDirectory, AssignmentsDirectory).Run(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	ctx := context.Background()
	{
		unreads := []UnreadAnnouncements{}
		if err := h.DB.Select(&unreads, "SELECT * FROM `unread_announcements`"); err != nil {
			return c.NoContent(334)
		}

		// rdb0
		rdb0.FlushDB(ctx)
		pipe := rdb0.Pipeline()
		defer pipe.Close()
		for _, unread := range unreads {
			if unread.IsDeleted {
				continue
			}
			pipe.SAdd(ctx, unread.UserID, unread.AnnouncementID)
		}
		pipe.Exec(ctx)
	}
	{
		courses := []Course{}
		if err := h.DB.Select(&courses, "SELECT * FROM `courses`"); err != nil {
			return c.NoContent(335)
		}

		// rdb1
		rdb1.FlushDB(ctx)
		pipe := rdb1.Pipeline()
		defer pipe.Close()
		for _, course := range courses {
			pipe.Set(ctx, course.ID, course.Status, 0)
		}
		pipe.Exec(ctx)
	}
	{
		rdb2.FlushDB(ctx)
	}
	{
		type Submission struct {
			UserID  string `db:"user_id"`
			ClassID string `db:"class_id"`
			// UserCode string `db:"user_code"`
			// FileName string `db:"file_name"`
			// Score sql.NullInt64 `db:"score"`
		}
		submits := []Submission{}
		if err := h.DB.Select(&submits, "SELECT `user_id`, `class_id` from `submissions`"); err != nil {
			return c.NoContent(336)
		}

		// rdb3
		rdb3.FlushDB(ctx)
		pipe := rdb3.Pipeline()
		defer pipe.Close()
		for _, submit := range submits {
			score := 0
			if err := h.DB.Get(&score,
				"SELECT `score` FROM `submissions` WHERE `user_id` = ? AND `class_id` = ?",
				submit.UserID, submit.ClassID); err != nil {
				pipe.HSet(ctx, submit.ClassID, submit.UserID, "")
			} else {
				pipe.HSet(ctx, submit.ClassID, submit.UserID, strconv.Itoa(score))
			}
		}
		pipe.Exec(ctx)
	}
	{
		users := []User{}
		if err := h.DB.Select(&users, "SELECT * FROM `users`"); err != nil {
			return c.NoContent(337)
		}
		// rdb4
		rdb4.FlushDB(ctx)
		pipe := rdb4.Pipeline()
		defer pipe.Close()
		for _, user := range users {
			pipe.Set(ctx, user.Code, user.ID, 0)
		}
		pipe.Exec(ctx)
	}

	// ??????????????????????????????????????????????????????????????????????????????????????????
	sessionCache = sync.Map{}
	res := InitializeResponse{
		Language: "go",
	}
	return c.JSON(http.StatusOK, res)
}

// IsLoggedIn ?????????????????????middleware
func (h *handlers) IsLoggedIn(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		r := c.Request()
		cookie, err := r.Cookie(SessionName)
		if err != nil {
			return c.String(http.StatusUnauthorized, "You are not logged in.")
		}
		if _, ok := sessionCache.Load(cookie.Value); ok {
			return next(c)
		} else {
			sess, err := session.Get(SessionName, c)
			if err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}
			if sess.IsNew {
				return c.String(http.StatusUnauthorized, "You are not logged in.")
			}
			iUserID, ok := sess.Values["userID"]
			if !ok {
				return c.String(http.StatusUnauthorized, "You are not logged in.")
			}
			iUserName, ok := sess.Values["userName"]
			if !ok {
				iUserName = ""
			}
			iIsAdmin, ok := sess.Values["isAdmin"]
			if !ok {
				iIsAdmin = false
			}
			iUserCode, ok := sess.Values["userCode"]
			if !ok {
				iUserCode = ""
			}
			user := UserSession{
				iUserID.(string), iUserName.(string), iIsAdmin.(bool), iUserCode.(string),
			}
			sessionCache.Store(cookie.Value, user)
			return next(c)
		}
	}
}

// IsAdmin admin?????????middleware
func (h *handlers) IsAdmin(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		r := c.Request()
		cookie, err := r.Cookie(SessionName)
		if err != nil {
			return c.String(http.StatusForbidden, "You are not admin user.")
		}
		if iuser, ok := sessionCache.Load(cookie.Value); ok {
			user := iuser.(UserSession)
			if user.IsAdmin {
				return next(c)
			} else {
				return c.String(http.StatusForbidden, "You are not admin user.")
			}
		} else {
			sess, err := session.Get(SessionName, c)
			if err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}
			isAdmin, ok := sess.Values["isAdmin"]
			if !ok {
				c.Logger().Error("failed to get isAdmin from session")
				return c.NoContent(http.StatusInternalServerError)
			}
			if !isAdmin.(bool) {
				return c.String(http.StatusForbidden, "You are not admin user.")
			}
			return next(c)
		}
	}
}

func getUserInfo(c echo.Context) (userID string, userName string, isAdmin bool, userCode string, err error) {
	r := c.Request()
	cookie, err := r.Cookie(SessionName)
	if err != nil {
		return "", "", false, "", errors.New("failed to get userInfo from session")
	}
	if iuser, ok := sessionCache.Load(cookie.Value); ok {
		val := iuser.(UserSession)
		return val.UserID, val.UserName, val.IsAdmin, val.UserCode, nil
	} else {
		sess, err := session.Get(SessionName, c)
		if err != nil {
			return "", "", false, "", err
		}
		_userID, ok := sess.Values["userID"]
		if !ok {
			return "", "", false, "", errors.New("failed to get userID from session")
		}
		_userName, ok := sess.Values["userName"]
		if !ok {
			return "", "", false, "", errors.New("failed to get userName from session")
		}
		_isAdmin, ok := sess.Values["isAdmin"]
		if !ok {
			return "", "", false, "", errors.New("failed to get isAdmin from session")
		}
		_userCode, ok := sess.Values["userCode"]
		if !ok {
			return "", "", false, "", errors.New("failed to get userCode from session")
		}

		user := UserSession{
			_userID.(string), _userName.(string), _isAdmin.(bool), _userCode.(string),
		}
		sessionCache.Store(cookie.Value, user)
		return _userID.(string), _userName.(string), _isAdmin.(bool), _userCode.(string), nil
	}
}

type UserType string

const (
	Student UserType = "student"
	Teacher UserType = "teacher"
)

type User struct {
	ID             string   `db:"id"`
	Code           string   `db:"code"`
	Name           string   `db:"name"`
	HashedPassword []byte   `db:"hashed_password"`
	Type           UserType `db:"type"`
}

type CourseType = string

const (
	LiberalArts   CourseType = "liberal-arts"
	MajorSubjects CourseType = "major-subjects"
)

type DayOfWeek = string

const (
	Monday    DayOfWeek = "monday"
	Tuesday   DayOfWeek = "tuesday"
	Wednesday DayOfWeek = "wednesday"
	Thursday  DayOfWeek = "thursday"
	Friday    DayOfWeek = "friday"
)

var daysOfWeek = []DayOfWeek{Monday, Tuesday, Wednesday, Thursday, Friday}

type CourseStatus = string

const (
	StatusRegistration CourseStatus = "registration"
	StatusInProgress   CourseStatus = "in-progress"
	StatusClosed       CourseStatus = "closed"
)

type Course struct {
	ID          string       `db:"id"`
	Code        string       `db:"code"`
	Type        CourseType   `db:"type"`
	Name        string       `db:"name"`
	Description string       `db:"description"`
	Credit      uint8        `db:"credit"`
	Period      uint8        `db:"period"`
	DayOfWeek   DayOfWeek    `db:"day_of_week"`
	TeacherID   string       `db:"teacher_id"`
	Keywords    string       `db:"keywords"`
	Status      CourseStatus `db:"status"`
}

// ---------- Public API ----------

type LoginRequest struct {
	Code     string `json:"code"`
	Password string `json:"password"`
}

// Login POST /login ????????????
func (h *handlers) Login(c echo.Context) error {
	var req LoginRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	var user User
	if err := h.DB.Get(&user, "SELECT * FROM `users` WHERE `code` = ?", req.Code); err != nil && err != sql.ErrNoRows {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	} else if err == sql.ErrNoRows {
		return c.String(http.StatusUnauthorized, "Code or Password is wrong.")
	}

	if bcrypt.CompareHashAndPassword(user.HashedPassword, []byte(req.Password)) != nil {
		return c.String(http.StatusUnauthorized, "Code or Password is wrong.")
	}

	sess, err := session.Get(SessionName, c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if userID, ok := sess.Values["userID"].(string); ok && userID == user.ID {
		return c.String(http.StatusBadRequest, "You are already logged in.")
	}

	sess.Values["userID"] = user.ID
	sess.Values["userName"] = user.Name
	sess.Values["isAdmin"] = user.Type == Teacher
	sess.Values["userCode"] = user.Code
	sess.Options = &sessions.Options{
		Path:   "/",
		MaxAge: 3600,
	}

	if err := sess.Save(c.Request(), c.Response()); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	r := c.Request()
	cookie, err := r.Cookie(SessionName)
	if err == nil {
		user := UserSession{user.ID, user.Name, user.Type == Teacher, user.Code}
		sessionCache.Store(cookie.Value, user)
	}
	return c.NoContent(http.StatusOK)
}

// Logout POST /logout ???????????????
func (h *handlers) Logout(c echo.Context) error {
	sess, err := session.Get(SessionName, c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	sess.Options = &sessions.Options{
		Path:   "/",
		MaxAge: -1,
	}

	if err := sess.Save(c.Request(), c.Response()); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusOK)
}

// ---------- Users API ----------

type GetMeResponse struct {
	Code    string `json:"code"`
	Name    string `json:"name"`
	IsAdmin bool   `json:"is_admin"`
}

// GetMe GET /api/users/me ????????????????????????
func (h *handlers) GetMe(c echo.Context) error {
	_, userName, isAdmin, userCode, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// var userCode string
	// if err := h.DB.Get(&userCode, "SELECT `code` FROM `users` WHERE `id` = ?", userID); err != nil {
	// 	c.Logger().Error(err)
	// 	return c.NoContent(http.StatusInternalServerError)
	// }

	return c.JSON(http.StatusOK, GetMeResponse{
		Code:    userCode,
		Name:    userName,
		IsAdmin: isAdmin,
	})
}

type GetRegisteredCourseResponseContent struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Teacher   string    `json:"teacher"`
	Period    uint8     `json:"period"`
	DayOfWeek DayOfWeek `json:"day_of_week"`
}

// GetRegisteredCourses GET /api/users/me/courses ??????????????????????????????
func (h *handlers) GetRegisteredCourses(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	tx, err := h.DB.Beginx()
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	var courses []Course
	query := "SELECT `courses`.*" +
		" FROM `courses`" +
		" JOIN `registrations` ON `courses`.`id` = `registrations`.`course_id`" +
		" WHERE `courses`.`status` != ? AND `registrations`.`user_id` = ?"
	if err := tx.Select(&courses, query, StatusClosed, userID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// ???????????????0??????????????????????????????
	res := make([]GetRegisteredCourseResponseContent, 0, len(courses))
	for _, course := range courses {
		var teacher User
		// TODO: N+1 (gnu)
		if err := tx.Get(&teacher, "SELECT * FROM `users` WHERE `id` = ?", course.TeacherID); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}

		res = append(res, GetRegisteredCourseResponseContent{
			ID:        course.ID,
			Name:      course.Name,
			Teacher:   teacher.Name,
			Period:    course.Period,
			DayOfWeek: course.DayOfWeek,
		})
	}

	if err := tx.Commit(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusOK, res)
}

type RegisterCourseRequestContent struct {
	ID string `json:"id"`
}

type RegisterCoursesErrorResponse struct {
	CourseNotFound       []string `json:"course_not_found,omitempty"`
	NotRegistrableStatus []string `json:"not_registrable_status,omitempty"`
	ScheduleConflict     []string `json:"schedule_conflict,omitempty"`
}

// RegisterCourses PUT /api/users/me/courses ????????????
func (h *handlers) RegisterCourses(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var req []RegisterCourseRequestContent
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}
	sort.Slice(req, func(i, j int) bool {
		return req[i].ID < req[j].ID
	})

	tx, err := h.DB.Beginx()
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	var errors RegisterCoursesErrorResponse
	var newlyAdded []Course
	for _, courseReq := range req {
		courseID := courseReq.ID
		var course Course
		// TODO: N+1 (gnu)
		if err := tx.Get(&course, "SELECT * FROM `courses` WHERE `id` = ? FOR SHARE", courseID); err != nil && err != sql.ErrNoRows {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		} else if err == sql.ErrNoRows {
			errors.CourseNotFound = append(errors.CourseNotFound, courseReq.ID)
			continue
		}

		if course.Status != StatusRegistration {
			errors.NotRegistrableStatus = append(errors.NotRegistrableStatus, course.ID)
			continue
		}

		// ???????????????????????????????????????????????????
		var count int
		// TODO: N+1 (gnu)
		if err := tx.Get(&count, "SELECT COUNT(*) FROM `registrations` WHERE `course_id` = ? AND `user_id` = ?", course.ID, userID); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		if count > 0 {
			continue
		}

		newlyAdded = append(newlyAdded, course)
	}

	var alreadyRegistered []Course
	query := "SELECT `courses`.*" +
		" FROM `courses`" +
		" JOIN `registrations` ON `courses`.`id` = `registrations`.`course_id`" +
		" WHERE `courses`.`status` != ? AND `registrations`.`user_id` = ?"
	if err := tx.Select(&alreadyRegistered, query, StatusClosed, userID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	alreadyRegistered = append(alreadyRegistered, newlyAdded...)
	for _, course1 := range newlyAdded {
		for _, course2 := range alreadyRegistered {
			if course1.ID != course2.ID && course1.Period == course2.Period && course1.DayOfWeek == course2.DayOfWeek {
				errors.ScheduleConflict = append(errors.ScheduleConflict, course1.ID)
				break
			}
		}
	}

	if len(errors.CourseNotFound) > 0 || len(errors.NotRegistrableStatus) > 0 || len(errors.ScheduleConflict) > 0 {
		return c.JSON(http.StatusBadRequest, errors)
	}

	for _, course := range newlyAdded {
		// TODO: N+1 (gnu)
		_, err = tx.Exec("INSERT INTO `registrations` (`course_id`, `user_id`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `course_id` = VALUES(`course_id`), `user_id` = VALUES(`user_id`)", course.ID, userID)
		if err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}

	if err = tx.Commit(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusOK)
}

type Class struct {
	ID               *string `db:"id"`
	CourseID         *string `db:"course_id"`
	Part             *uint8  `db:"part"`
	Title            *string `db:"title"`
	Description      *string `db:"description"`
	SubmissionClosed *bool   `db:"submission_closed"`
}

type GetGradeResponse struct {
	Summary       Summary        `json:"summary"`
	CourseResults []CourseResult `json:"courses"`
}

type Summary struct {
	Credits   int     `json:"credits"`
	GPA       float64 `json:"gpa"`
	GpaTScore float64 `json:"gpa_t_score"` // ?????????
	GpaAvg    float64 `json:"gpa_avg"`     // ?????????
	GpaMax    float64 `json:"gpa_max"`     // ?????????
	GpaMin    float64 `json:"gpa_min"`     // ?????????
}

type CourseResult struct {
	Name             string       `json:"name"`
	Code             string       `json:"code"`
	TotalScore       int          `json:"total_score"`
	TotalScoreTScore float64      `json:"total_score_t_score"` // ?????????
	TotalScoreAvg    float64      `json:"total_score_avg"`     // ?????????
	TotalScoreMax    int          `json:"total_score_max"`     // ?????????
	TotalScoreMin    int          `json:"total_score_min"`     // ?????????
	ClassScores      []ClassScore `json:"class_scores"`
}

type ClassScore struct {
	ClassID    string `json:"class_id"`
	Title      string `json:"title"`
	Part       uint8  `json:"part"`
	Score      *int   `json:"score"`      // 0~100???
	Submitters int    `json:"submitters"` // ?????????????????????
}

type ClassWithCourse struct {
	Class  Class  `db:"classes"`
	Course Course `db:"courses"`
}

type classIdNameCode struct {
	ID     string
	Name   string
	Code   string
	Status CourseStatus
	Credit uint8
}

type CourseResultWithMyTotalScore struct {
	classScores  []ClassScore
	Course       Course
	myTotalScore int
}

var gpaGroup singleflight.Group
var mu *sync.Mutex = new(sync.Mutex)
var cond *sync.Cond = sync.NewCond(mu)

const gpaSyncInterval = 1 * time.Second

func syncGPA() {
	tick := time.Tick(gpaSyncInterval)
	for {
		select {
		case <-tick:
			gpaGroup.Forget("")
			cond.Broadcast()
		}
	}
}

// GetGrades GET /api/users/me/grades ????????????
func (h *handlers) GetGrades(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// ????????????????????????????????????
	var registeredClasses []ClassWithCourse
	query := "SELECT `classes`.`id` AS `classes.id`,`classes`.`course_id` AS `classes.course_id`, `classes`.`part` AS `classes.part`, `classes`.`title` AS `classes.title`, `classes`.`description` AS `classes.description`, `classes`.`submission_closed` AS `classes.submission_closed`, `courses`.`id` AS `courses.id`, `courses`.`code` AS `courses.code`, `courses`.`type` AS `courses.type`, `courses`.`name` AS `courses.name`, `courses`.`description` AS `courses.description`, `courses`.`credit` AS `courses.credit`, `courses`.`period` AS `courses.period`, `courses`.`day_of_week` AS `courses.day_of_week`, `courses`.`teacher_id` AS `courses.teacher_id`, `courses`.`keywords` AS `courses.keywords`, `courses`.`status` AS `courses.status`" +
		" FROM `registrations`" +
		" JOIN `courses` ON `registrations`.`course_id` = `courses`.`id`" +
		" LEFT JOIN `classes` ON `classes`.`course_id` = `courses`.`id`" +
		" WHERE `registrations`.`user_id` = ? ORDER BY `classes`.`part` DESC"
	if err := h.DB.Select(&registeredClasses, query, userID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// ??????????????????????????????
	myGPA := 0.0
	myCredits := 0
	courseDict := make(map[string]CourseResultWithMyTotalScore)
	t1 := time.Now()
	for _, classWithCourse := range registeredClasses {
		// ??????????????????????????????
		classScores, ok := courseDict[classWithCourse.Course.ID]
		if !ok {
			classScores = CourseResultWithMyTotalScore{make([]ClassScore, 0), Course{}, 0}
		}
		if classWithCourse.Class.ID != nil {
			got, err := rdb3.HGet(c.Request().Context(), *classWithCourse.Class.ID, userID).Result()
			var scorePtr *int = nil
			if err == nil {
				score, err := strconv.Atoi(got)
				if err == nil {
					classScores.myTotalScore += score
					scorePtr = &score
				}
			}
			submissionsCount, err := rdb3.HLen(c.Request().Context(), *classWithCourse.Class.ID).Result()
			if err != nil {
				submissionsCount = 0
			}
			classScores.classScores = append(classScores.classScores, ClassScore{
				ClassID:    *classWithCourse.Class.ID,
				Part:       *classWithCourse.Class.Part,
				Title:      *classWithCourse.Class.Title,
				Score:      scorePtr,
				Submitters: int(submissionsCount),
			})
		}
		classScores.Course = classWithCourse.Course
		courseDict[classWithCourse.Course.ID] = classScores
	}
	t2 := time.Now()
	courseResults := make([]CourseResult, 0, len(registeredClasses))
	for _, res := range courseDict {
		// ??????????????????????????????????????????TotalScore???????????????
		var totals []int
		query := "SELECT IFNULL(SUM(`submissions`.`score`), 0) AS `total_score`" +
			" FROM `users`" +
			" JOIN `registrations` ON `users`.`id` = `registrations`.`user_id`" +
			" JOIN `courses` ON `registrations`.`course_id` = `courses`.`id`" +
			" LEFT JOIN `classes` ON `courses`.`id` = `classes`.`course_id`" +
			" LEFT JOIN `submissions` ON `users`.`id` = `submissions`.`user_id` AND `submissions`.`class_id` = `classes`.`id`" +
			" WHERE `courses`.`id` = ?" +
			" GROUP BY `users`.`id`"
		if err := h.DB.Select(&totals, query, res.Course.ID); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
		courseResults = append(courseResults, CourseResult{
			Name:             res.Course.Name,
			Code:             res.Course.Code,
			TotalScore:       res.myTotalScore,
			TotalScoreTScore: tScoreInt(res.myTotalScore, totals),
			TotalScoreAvg:    averageInt(totals, 0),
			TotalScoreMax:    maxInt(totals, 0),
			TotalScoreMin:    minInt(totals, 0),
			ClassScores:      res.classScores,
		})

		// ?????????GPA??????
		if res.Course.Status == StatusClosed {
			myGPA += float64(res.myTotalScore * int(res.Course.Credit))
			myCredits += int(res.Course.Credit)
		}
	}
	t3 := time.Now()
	if myCredits > 0 {
		myGPA = myGPA / 100 / float64(myCredits)
	}

	// GPA????????????
	// ????????????????????????????????????????????????GPA??????
	mu.Lock()
	cond.Wait()
	mu.Unlock()
	gpas, err := getGPA(h, c)
	t4 := time.Now()

	res := GetGradeResponse{
		Summary: Summary{
			Credits:   myCredits,
			GPA:       myGPA,
			GpaTScore: tScoreFloat64(myGPA, gpas),
			GpaAvg:    averageFloat64(gpas, 0),
			GpaMax:    maxFloat64(gpas, 0),
			GpaMin:    minFloat64(gpas, 0),
		},
		CourseResults: courseResults,
	}
	c.Logger().Info(fmt.Sprintf("[GRADE] b1:%d\tb2:%d\tb3:%d", t2.Sub(t1)/time.Millisecond, t3.Sub(t2)/time.Millisecond, t4.Sub(t3)/time.Millisecond))

	return c.JSON(http.StatusOK, res)
}

func getGPA(h *handlers, c echo.Context) ([]float64, error) {
	gpas, err, _ := gpaGroup.Do("", func() (interface{}, error) {
		gpas, err := getGpa(h, c)
		return gpas, err
	})
	if err != nil {
		return nil, err
	}
	return gpas.([]float64), nil
}

func getGpa(h *handlers, c echo.Context) ([]float64, error) {
	var gpas []float64
	query := "SELECT IFNULL(SUM(`submissions`.`score` * `courses`.`credit`), 0) / 100 / `credits`.`credits` AS `gpa`" +
		" FROM `users`" +
		" JOIN (" +
		"     SELECT `users`.`id` AS `user_id`, SUM(`courses`.`credit`) AS `credits`" +
		"     FROM `users`" +
		"     JOIN `registrations` ON `users`.`id` = `registrations`.`user_id`" +
		"     JOIN `courses` ON `registrations`.`course_id` = `courses`.`id` AND `courses`.`status` = ?" +
		"     GROUP BY `users`.`id`" +
		" ) AS `credits` ON `credits`.`user_id` = `users`.`id`" +
		" JOIN `registrations` ON `users`.`id` = `registrations`.`user_id`" +
		" JOIN `courses` ON `registrations`.`course_id` = `courses`.`id` AND `courses`.`status` = ?" +
		" LEFT JOIN `classes` ON `courses`.`id` = `classes`.`course_id`" +
		" LEFT JOIN `submissions` ON `users`.`id` = `submissions`.`user_id` AND `submissions`.`class_id` = `classes`.`id`" +
		" WHERE `users`.`type` = ?" +
		" GROUP BY `users`.`id`"
	if err := h.DB.Select(&gpas, query, StatusClosed, StatusClosed, Student); err != nil {
		c.Logger().Error(err)
		return nil, fmt.Errorf("hoge %w", err)
	}
	return gpas, nil
}

// ---------- Courses API ----------

// SearchCourses GET /api/courses ????????????
func (h *handlers) SearchCourses(c echo.Context) error {
	query := "SELECT `courses`.*, `users`.`name` AS `teacher`" +
		" FROM `courses` JOIN `users` ON `courses`.`teacher_id` = `users`.`id`" +
		" WHERE 1=1"
	var condition string
	var args []interface{}

	// ???????????????????????????????????????????????????????????????

	if courseType := c.QueryParam("type"); courseType != "" {
		condition += " AND `courses`.`type` = ?"
		args = append(args, courseType)
	}

	if credit, err := strconv.Atoi(c.QueryParam("credit")); err == nil && credit > 0 {
		condition += " AND `courses`.`credit` = ?"
		args = append(args, credit)
	}

	if teacher := c.QueryParam("teacher"); teacher != "" {
		condition += " AND `users`.`name` = ?"
		args = append(args, teacher)
	}

	if period, err := strconv.Atoi(c.QueryParam("period")); err == nil && period > 0 {
		condition += " AND `courses`.`period` = ?"
		args = append(args, period)
	}

	if dayOfWeek := c.QueryParam("day_of_week"); dayOfWeek != "" {
		condition += " AND `courses`.`day_of_week` = ?"
		args = append(args, dayOfWeek)
	}

	if keywords := c.QueryParam("keywords"); keywords != "" {
		arr := strings.Split(keywords, " ")
		var nameCondition string
		for _, keyword := range arr {
			nameCondition += " AND `courses`.`name` LIKE ?"
			args = append(args, "%"+keyword+"%")
		}
		var keywordsCondition string
		for _, keyword := range arr {
			keywordsCondition += " AND `courses`.`keywords` LIKE ?"
			args = append(args, "%"+keyword+"%")
		}
		condition += fmt.Sprintf(" AND ((1=1%s) OR (1=1%s))", nameCondition, keywordsCondition)
	}

	if status := c.QueryParam("status"); status != "" {
		condition += " AND `courses`.`status` = ?"
		args = append(args, status)
	}

	condition += " ORDER BY `courses`.`code`"

	var page int
	if c.QueryParam("page") == "" {
		page = 1
	} else {
		var err error
		page, err = strconv.Atoi(c.QueryParam("page"))
		if err != nil || page <= 0 {
			return c.String(http.StatusBadRequest, "Invalid page.")
		}
	}
	limit := 20
	offset := limit * (page - 1)

	// limit??????????????????????????????????????????limit?????????????????????????????????????????????????????????????????????????????????
	condition += " LIMIT ? OFFSET ?"
	args = append(args, limit+1, offset)

	// ?????????0??????????????????????????????
	res := make([]GetCourseDetailResponse, 0)
	if err := h.DB.Select(&res, query+condition, args...); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var links []string
	linkURL, err := url.Parse(c.Request().URL.Path + "?" + c.Request().URL.RawQuery)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	q := linkURL.Query()
	if page > 1 {
		q.Set("page", strconv.Itoa(page-1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"prev\"", linkURL))
	}
	if len(res) > limit {
		q.Set("page", strconv.Itoa(page+1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"next\"", linkURL))
	}
	if len(links) > 0 {
		c.Response().Header().Set("Link", strings.Join(links, ","))
	}

	if len(res) == limit+1 {
		res = res[:len(res)-1]
	}

	return c.JSON(http.StatusOK, res)
}

type AddCourseRequest struct {
	Code        string     `json:"code"`
	Type        CourseType `json:"type"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Credit      int        `json:"credit"`
	Period      int        `json:"period"`
	DayOfWeek   DayOfWeek  `json:"day_of_week"`
	Keywords    string     `json:"keywords"`
}

type AddCourseResponse struct {
	ID string `json:"id"`
}

// AddCourse POST /api/courses ??????????????????
func (h *handlers) AddCourse(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var req AddCourseRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	if req.Type != LiberalArts && req.Type != MajorSubjects {
		return c.String(http.StatusBadRequest, "Invalid course type.")
	}
	if !contains(daysOfWeek, req.DayOfWeek) {
		return c.String(http.StatusBadRequest, "Invalid day of week.")
	}

	courseID := newULID()
	_, err = h.DB.Exec("INSERT INTO `courses` (`id`, `code`, `type`, `name`, `description`, `credit`, `period`, `day_of_week`, `teacher_id`, `keywords`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		courseID, req.Code, req.Type, req.Name, req.Description, req.Credit, req.Period, req.DayOfWeek, userID, req.Keywords)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == uint16(mysqlErrNumDuplicateEntry) {
			var course Course
			if err := h.DB.Get(&course, "SELECT * FROM `courses` WHERE `code` = ?", req.Code); err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}
			if req.Type != course.Type || req.Name != course.Name || req.Description != course.Description || req.Credit != int(course.Credit) || req.Period != int(course.Period) || req.DayOfWeek != course.DayOfWeek || req.Keywords != course.Keywords {
				return c.String(http.StatusConflict, "A course with the same code already exists.")
			}

			rdb1.Set(c.Request().Context(), courseID, StatusRegistration, 0)
			return c.JSON(http.StatusCreated, AddCourseResponse{ID: course.ID})
		}
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	rdb1.Set(c.Request().Context(), courseID, StatusRegistration, 0)
	return c.JSON(http.StatusCreated, AddCourseResponse{ID: courseID})
}

type GetCourseDetailResponse struct {
	ID          string       `json:"id" db:"id"`
	Code        string       `json:"code" db:"code"`
	Type        string       `json:"type" db:"type"`
	Name        string       `json:"name" db:"name"`
	Description string       `json:"description" db:"description"`
	Credit      uint8        `json:"credit" db:"credit"`
	Period      uint8        `json:"period" db:"period"`
	DayOfWeek   string       `json:"day_of_week" db:"day_of_week"`
	TeacherID   string       `json:"-" db:"teacher_id"`
	Keywords    string       `json:"keywords" db:"keywords"`
	Status      CourseStatus `json:"status" db:"status"`
	Teacher     string       `json:"teacher" db:"teacher"`
}

// GetCourseDetail GET /api/courses/:courseID ?????????????????????
func (h *handlers) GetCourseDetail(c echo.Context) error {
	courseID := c.Param("courseID")

	var res GetCourseDetailResponse
	query := "SELECT `courses`.*, `users`.`name` AS `teacher`" +
		" FROM `courses`" +
		" JOIN `users` ON `courses`.`teacher_id` = `users`.`id`" +
		" WHERE `courses`.`id` = ?"
	if err := h.DB.Get(&res, query, courseID); err != nil && err != sql.ErrNoRows {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	} else if err == sql.ErrNoRows {
		return c.String(http.StatusNotFound, "No such course.")
	}

	return c.JSON(http.StatusOK, res)
}

type SetCourseStatusRequest struct {
	Status CourseStatus `json:"status"`
}

// SetCourseStatus PUT /api/courses/:courseID/status ?????????????????????????????????
func (h *handlers) SetCourseStatus(c echo.Context) error {
	courseID := c.Param("courseID")

	var req SetCourseStatusRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}
	// courses???insert???????????????Transaction?????????
	if rdb1.Get(c.Request().Context(), courseID).Err() != nil {
		return c.String(http.StatusNotFound, "No such course.")
	}

	if _, err := h.DB.Exec("UPDATE `courses` SET `status` = ? WHERE `id` = ?", req.Status, courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if err := rdb1.Set(c.Request().Context(), courseID, req.Status, 0).Err(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.NoContent(http.StatusOK)
}

type ClassWithSubmitted struct {
	ID               string `db:"id"`
	CourseID         string `db:"course_id"`
	Part             uint8  `db:"part"`
	Title            string `db:"title"`
	Description      string `db:"description"`
	SubmissionClosed bool   `db:"submission_closed"`
	Submitted        bool   `db:"submitted"`
}

type GetClassResponse struct {
	ID               string `json:"id"`
	Part             uint8  `json:"part"`
	Title            string `json:"title"`
	Description      string `json:"description"`
	SubmissionClosed bool   `json:"submission_closed"`
	Submitted        bool   `json:"submitted"`
}

// GetClasses GET /api/courses/:courseID/classes ???????????????????????????????????????
func (h *handlers) GetClasses(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseID := c.Param("courseID")
	// courses ???????????????????????????????????????????????????????????????Transaction?????????
	if rdb1.Get(c.Request().Context(), courseID).Err() != nil {
		return c.String(http.StatusNotFound, "No such course.")
	}
	classes := []Class{}
	if err := h.DB.Select(&classes,
		"SELECT * FROM `classes` WHERE `classes`.`course_id` = ? ORDER BY `classes`.`part`",
		courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	pipe := rdb3.Pipeline()
	defer pipe.Close()
	pScores := []*redis.StringCmd{}
	for _, class := range classes {
		pScores = append(pScores, pipe.HGet(c.Request().Context(), *class.ID, userID))
	}
	pipe.Exec(c.Request().Context())
	// ?????????0??????????????????????????????
	res := make([]GetClassResponse, 0, len(classes))
	for i, class := range classes {
		submitted := false
		if pScores[i].Err() == nil && pScores[i].Val() != "" {
			submitted = true
		}
		res = append(res, GetClassResponse{
			ID:               *class.ID,
			Part:             *class.Part,
			Title:            *class.Title,
			Description:      *class.Description,
			SubmissionClosed: *class.SubmissionClosed,
			Submitted:        submitted,
		})
	}
	return c.JSON(http.StatusOK, res)
}

type AddClassRequest struct {
	Part        uint8  `json:"part"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type AddClassResponse struct {
	ClassID string `json:"class_id"`
}

// AddClass POST /api/courses/:courseID/classes ????????????(&??????)??????
func (h *handlers) AddClass(c echo.Context) error {
	courseID := c.Param("courseID")

	var req AddClassRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	tx, err := h.DB.Beginx()
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	var course Course
	if err := tx.Get(&course, "SELECT * FROM `courses` WHERE `id` = ? FOR SHARE", courseID); err != nil && err != sql.ErrNoRows {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	} else if err == sql.ErrNoRows {
		return c.String(http.StatusNotFound, "No such course.")
	}
	if course.Status != StatusInProgress {
		return c.String(http.StatusBadRequest, "This course is not in-progress.")
	}

	classID := newULID()
	if _, err := tx.Exec("INSERT INTO `classes` (`id`, `course_id`, `part`, `title`, `description`) VALUES (?, ?, ?, ?, ?)",
		classID, courseID, req.Part, req.Title, req.Description); err != nil {
		_ = tx.Rollback()
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == uint16(mysqlErrNumDuplicateEntry) {
			var class Class
			if err := h.DB.Get(&class, "SELECT * FROM `classes` WHERE `course_id` = ? AND `part` = ?", courseID, req.Part); err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}
			if req.Title != *class.Title || req.Description != *class.Description {
				return c.String(http.StatusConflict, "A class with the same part already exists.")
			}
			return c.JSON(http.StatusCreated, AddClassResponse{ClassID: *class.ID})
		}
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if err := tx.Commit(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusCreated, AddClassResponse{ClassID: classID})
}

func myReadAll(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 50_000)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}

// SubmitAssignment POST /api/courses/:courseID/classes/:classID/assignments ???????????????
func (h *handlers) SubmitAssignment(c echo.Context) error {
	userID, _, _, userCode, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseID := c.Param("courseID")
	classID := c.Param("classID")

	registrationCountCh := make(chan int, 2)
	go func() {
		// ??????????????????????????? :: course???class???????????????????????????????????????????????????????????????????????????
		var registrationCount int
		if err := h.DB.Get(&registrationCount, "SELECT 1 FROM `registrations` WHERE `user_id` = ? AND `course_id` = ? LIMIT 1", userID, courseID); err != nil {
			c.Logger().Error(err)
			if errors.Is(err, sql.ErrNoRows) {
				registrationCountCh <- 0
				return
			}
			registrationCountCh <- -1
			return
		}
		registrationCountCh <- registrationCount
	}()

	type FormFile struct {
		data     []byte
		filename string
		err      error
	}
	formFileCh := make(chan FormFile, 2)
	go func() {
		file, header, err := c.Request().FormFile("file")
		if err != nil {
			formFileCh <- FormFile{err: err}
			return
		}
		defer file.Close()

		data, err := myReadAll(file)
		formFileCh <- FormFile{data: data, filename: header.Filename, err: nil}
	}()

	status, err := rdb1.Get(c.Request().Context(), courseID).Result()
	if err != nil {
		return c.String(http.StatusNotFound, "No such course.")
	} else {
		if status != StatusInProgress {
			return c.String(http.StatusBadRequest, "This course is not in progress.")
		}
	}

	submissionClosedString, err := rdb2.Get(c.Request().Context(), classID).Result()
	if err != nil {
		if err == redis.Nil {
			var submissionClosed bool
			if err := h.DB.Get(&submissionClosed, "SELECT `submission_closed` FROM `classes` WHERE `id` = ?", classID); err != nil && err != sql.ErrNoRows {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			} else if err == sql.ErrNoRows {
				return c.String(http.StatusNotFound, "No such class.")
			}
			_, err = rdb2.Set(c.Request().Context(), classID, strconv.FormatBool(submissionClosed), 0).Result()
			if err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}
			if submissionClosed {
				return c.String(http.StatusBadRequest, "Submission has been closed for this class.")
			}
		}
	} else {
		if submissionClosedString == "true" {
			return c.String(http.StatusBadRequest, "Submission has been closed for this class.")
		}
	}

	registrationCount := <-registrationCountCh
	if registrationCount == 0 {
		return c.String(http.StatusBadRequest, "You have not taken this course.")
	} else if registrationCount < 0 {
		return c.NoContent(http.StatusInternalServerError)
	}

	formFile := <-formFileCh
	if formFile.err != nil {
		c.Logger().Error(formFile.err)
		return c.String(http.StatusBadRequest, "Invalid file.")
	}

	if _, err := h.DB.Exec("INSERT INTO `submissions` (`user_id`, `user_code`, `class_id`, `file_name`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `file_name` = VALUES(`file_name`)", userID, userCode, classID, formFile.filename); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	rdb3.HSet(c.Request().Context(), classID, userID, "")

	go func() {
		dst := AssignmentsDirectory + classID + "-" + userID + ".pdf"
		if err := os.WriteFile(dst, formFile.data, 0666); err != nil {
			c.Logger().Error(err)
			// return c.NoContent(http.StatusInternalServerError)
		}
	}()

	return c.NoContent(http.StatusNoContent)
}

type Score struct {
	UserCode string `json:"user_code"`
	Score    int    `json:"score"`
}

// RegisterScores PUT /api/courses/:courseID/classes/:classID/assignments/scores ??????????????????
func (h *handlers) RegisterScores(c echo.Context) error {
	var req []Score
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	classID := c.Param("classID")

	var submissionClosed bool
	if err := h.DB.Get(&submissionClosed, "SELECT `submission_closed` FROM `classes` WHERE `id` = ? FOR SHARE", classID); err != nil && err != sql.ErrNoRows {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	} else if err == sql.ErrNoRows {
		return c.String(http.StatusNotFound, "No such class.")
	}

	if !submissionClosed {
		return c.String(http.StatusBadRequest, "This assignment is not closed yet.")
	}
	// UPDATE example SET `name` = "Eve" WHERE `id` = 1;
	// UPDATE example SET `name` = "Frank" WHERE `id` = 2;
	// UPDATE example SET `name` = "Greg" WHERE `id` = 3;
	// UPDATE example SET `name` = "Helen" WHERE `id` = 4;
	// 	UPDATE `example` SET
	// name = ELT(FIELD(id,2,4,5),'Mary','Nancy','Oliver')
	// WHERE id IN (2,4,5)
	{
		scores := ""
		userCodes := ""
		for i, score := range req {
			if i == 0 {
				scores = strings.Join([]string{scores, fmt.Sprintf("%d", score.Score)}, " ")
			} else {
				scores = strings.Join([]string{scores, fmt.Sprintf(", %d", score.Score)}, " ")
			}
			if i == 0 {
				userCodes = strings.Join([]string{userCodes, fmt.Sprintf("\"%s\"", score.UserCode)}, " ")
			} else {
				userCodes = strings.Join([]string{userCodes, fmt.Sprintf(", \"%s\"", score.UserCode)}, " ")
			}
		}
		if _, err := h.DB.Exec(fmt.Sprintf("update `submissions` set `score` = ELT(FIELD(`user_code`, %s), %s) WHERE `user_code` IN (%s) AND `class_id` = ?", userCodes, scores, userCodes), classID); err != nil {
			c.Logger().Error(err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}

	userCodes := []string{}
	for _, score := range req {
		userCodes = append(userCodes, score.UserCode)
	}
	pipe := rdb3.Pipeline()
	defer pipe.Close()
	for i, userId := range rdb4.MGet(c.Request().Context(), userCodes...).Val() {
		pipe.HSet(c.Request().Context(), classID, userId, strconv.Itoa(req[i].Score))
	}
	pipe.Exec(c.Request().Context())
	return c.NoContent(http.StatusNoContent)
}

type Submission struct {
	UserID   string `db:"user_id"`
	UserCode string `db:"user_code"`
	FileName string `db:"file_name"`
}

// DownloadSubmittedAssignments GET /api/courses/:courseID/classes/:classID/assignments/export ????????????????????????????????????zip?????????????????????????????????
func (h *handlers) DownloadSubmittedAssignments(c echo.Context) error {
	classID := c.Param("classID")
	// ????????????????????????????????????tx??????
	var classCount int
	if err := h.DB.Get(&classCount, "SELECT COUNT(*) FROM `classes` WHERE `id` = ?", classID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if classCount == 0 {
		return c.String(http.StatusNotFound, "No such class.")
	}

	// submission_closed ????????????????????????????????????tx??????
	var submissions []Submission
	query := "SELECT `submissions`.`user_id`, `submissions`.`file_name`, `users`.`code` AS `user_code`" +
		" FROM `submissions`" +
		" JOIN `users` ON `users`.`id` = `submissions`.`user_id`" +
		" WHERE `class_id` = ?"
	if err := h.DB.Select(&submissions, query, classID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	zipFilePath := AssignmentsDirectory + classID + ".zip"
	if err := createSubmissionsZip(zipFilePath, classID, submissions); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if _, err := h.DB.Exec("UPDATE `classes` SET `submission_closed` = true WHERE `id` = ?", classID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if _, err := rdb2.Set(c.Request().Context(), classID, strconv.FormatBool(true), 0).Result(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.File(zipFilePath)
}
func createSubmissionsZip(zipFilePath string, classID string, submissions []Submission) error {
	tmpDir := AssignmentsDirectory + classID + "/"
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	if err := os.Mkdir(tmpDir, 0755); err != nil {
		return err
	}

	// ??????????????????????????????????????????
	for _, submission := range submissions {
		src := AssignmentsDirectory + classID + "-" + submission.UserID + ".pdf"
		dst := tmpDir + submission.UserCode + "-" + submission.FileName
		if err := os.Symlink(src, dst); err != nil {
			return err
		}
	}

	// -i 'tmpDir/*': ???zip?????????
	// -y : symbolic link
	// -1 : faster
	// TODO: no stdout
	return exec.Command("zip", "-j", "-r", "-q", "-0", zipFilePath, tmpDir, "-i", tmpDir+"*").Run()
}

// ---------- Announcement API ----------

type AnnouncementWithoutDetail struct {
	ID         string `json:"id" db:"id"`
	CourseID   string `json:"course_id" db:"course_id"`
	CourseName string `json:"course_name" db:"course_name"`
	Title      string `json:"title" db:"title"`
	Unread     bool   `json:"unread" db:"unread"`
}

type GetAnnouncementsResponse struct {
	UnreadCount   int                         `json:"unread_count"`
	Announcements []AnnouncementWithoutDetail `json:"announcements"`
}

// GetAnnouncementList GET /api/announcements ????????????????????????
func (h *handlers) GetAnnouncementList(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var announcements []AnnouncementWithoutDetail
	var args []interface{}
	query := "SELECT `announcements`.`id`, `courses`.`id` AS `course_id`, `courses`.`name` AS `course_name`, `announcements`.`title`" +
		" FROM `announcements`" +
		" JOIN `courses` ON `announcements`.`course_id` = `courses`.`id`" +
		" JOIN `registrations` ON `courses`.`id` = `registrations`.`course_id`" +
		" WHERE 1=1"

	if courseID := c.QueryParam("course_id"); courseID != "" {
		query += " AND `announcements`.`course_id` = ?"
		args = append(args, courseID)
	}

	query += " AND `registrations`.`user_id` = ?" +
		" ORDER BY `announcements`.`id` DESC" +
		" LIMIT ? OFFSET ?"
	args = append(args, userID)

	var page int
	if c.QueryParam("page") == "" {
		page = 1
	} else {
		page, err = strconv.Atoi(c.QueryParam("page"))
		if err != nil || page <= 0 {
			return c.String(http.StatusBadRequest, "Invalid page.")
		}
	}
	limit := 20
	offset := limit * (page - 1)
	// limit??????????????????????????????????????????limit?????????????????????????????????????????????????????????????????????????????????
	args = append(args, limit+1, offset)

	if err := h.DB.Select(&announcements, query, args...); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var links []string
	linkURL, err := url.Parse(c.Request().URL.Path + "?" + c.Request().URL.RawQuery)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	q := linkURL.Query()
	if page > 1 {
		q.Set("page", strconv.Itoa(page-1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"prev\"", linkURL))
	}
	if len(announcements) > limit {
		q.Set("page", strconv.Itoa(page+1))
		linkURL.RawQuery = q.Encode()
		links = append(links, fmt.Sprintf("<%v>; rel=\"next\"", linkURL))
	}
	if len(links) > 0 {
		c.Response().Header().Set("Link", strings.Join(links, ","))
	}

	if len(announcements) == limit+1 {
		announcements = announcements[:len(announcements)-1]
	}
	ctx := c.Request().Context()
	unreadCount := 0
	unreadMap := map[string]bool{}
	for _, v := range rdb0.SMembers(ctx, userID).Val() {
		unreadCount += 1
		unreadMap[v] = true
	}
	for i, _ := range announcements {
		// ???????????????????????????
		// redis?????????????????????
		_, unread := unreadMap[announcements[i].ID]
		announcements[i].Unread = unread
	}

	// ???????????????????????????????????????0??????????????????????????????
	announcementsRes := append(make([]AnnouncementWithoutDetail, 0, len(announcements)), announcements...)

	return c.JSON(http.StatusOK, GetAnnouncementsResponse{
		UnreadCount:   unreadCount,
		Announcements: announcementsRes,
	})
}

type Announcement struct {
	ID       string `db:"id"`
	CourseID string `db:"course_id"`
	Title    string `db:"title"`
	Message  string `db:"message"`
}

type AddAnnouncementRequest struct {
	ID       string `json:"id"`
	CourseID string `json:"course_id"`
	Title    string `json:"title"`
	Message  string `json:"message"`
}

// AddAnnouncement POST /api/announcements ????????????????????????
func (h *handlers) AddAnnouncement(c echo.Context) error {
	var req AddAnnouncementRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}
	// courses ???????????????????????????????????????????????????????????????Transaction?????????
	if rdb1.Get(c.Request().Context(), req.CourseID).Err() != nil {
		return c.String(http.StatusNotFound, "No such course.")
	}

	tx, err := h.DB.Beginx()
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("INSERT INTO `announcements` (`id`, `course_id`, `title`, `message`) VALUES (?, ?, ?, ?)",
		req.ID, req.CourseID, req.Title, req.Message); err != nil {
		_ = tx.Rollback()
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == uint16(mysqlErrNumDuplicateEntry) {
			var announcement Announcement
			if err := h.DB.Get(&announcement, "SELECT * FROM `announcements` WHERE `id` = ?", req.ID); err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}
			if announcement.CourseID != req.CourseID || announcement.Title != req.Title || announcement.Message != req.Message {
				return c.String(http.StatusConflict, "An announcement with the same id already exists.")
			}
			return c.NoContent(http.StatusCreated)
		}
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var targets []User
	query := "SELECT `users`.* FROM `users`" +
		" JOIN `registrations` ON `users`.`id` = `registrations`.`user_id`" +
		" WHERE `registrations`.`course_id` = ?"
	if err := tx.Select(&targets, query, req.CourseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	ctx := c.Request().Context()
	pipe := rdb0.Pipeline()
	defer pipe.Close()
	for _, user := range targets {
		pipe.SAdd(ctx, user.ID, req.ID)
	}
	pipe.Exec(ctx)

	if err := tx.Commit(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusCreated)
}

type AnnouncementDetail struct {
	ID         string `json:"id" db:"id"`
	CourseID   string `json:"course_id" db:"course_id"`
	CourseName string `json:"course_name" db:"course_name"`
	Title      string `json:"title" db:"title"`
	Message    string `json:"message" db:"message"`
	Unread     bool   `json:"unread" db:"unread"`
}

// GetAnnouncementDetail GET /api/announcements/:announcementID ????????????????????????
func (h *handlers) GetAnnouncementDetail(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	announcementID := c.Param("announcementID")

	// is_deleted?????????????????????????????????????????????????????????
	// unread_announcements ???insert??????
	var announcement AnnouncementDetail
	query := "SELECT `announcements`.`id`, `courses`.`id` AS `course_id`, `courses`.`name` AS `course_name`, `announcements`.`title`, `announcements`.`message`" +
		" FROM `announcements`" +
		" JOIN `courses` ON `courses`.`id` = `announcements`.`course_id`" +
		" JOIN `registrations` AS `r` ON `r`.`course_id` = `courses`.`id` AND `r`.`user_id` = ?" +
		" WHERE `announcements`.`id` = ?"
	if err := h.DB.Get(&announcement, query, userID, announcementID); err != nil && err != sql.ErrNoRows {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	} else if err == sql.ErrNoRows {
		return c.String(http.StatusNotFound, "No such announcement.")
	}
	ctx := c.Request().Context()
	deleteSuccess := rdb0.SRem(ctx, userID, announcementID).Val() > 0
	announcement.Unread = deleteSuccess
	return c.JSON(http.StatusOK, announcement)
}
