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
	AssignmentsDirectory      = "../assignments/"
	InitDataDirectory         = "../data/"
	SessionName               = "isucholar_go"
	mysqlErrNumDuplicateEntry = 1062
)

type handlers struct {
	DB *sqlx.DB
	Replica *sqlx.DB
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

var rdb0 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   0, // 0 - 15
})

// key: courseID value, status
var rdb1 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   1, // 0 - 15
})

// key: classID value, status
var rdb2 = redis.NewClient(&redis.Options{
	Addr: "172.31.47.193:6379",
	DB:   2, // 0 - 15
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
	replica, _ := GetReplica(false)
	db.SetMaxOpenConns(300)
	db.SetMaxIdleConns(300)
	replica.SetMaxOpenConns(300)
	replica.SetMaxIdleConns(300)

	h := &handlers{
		DB: db,
		Replica: replica,
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

// Initialize POST /initialize 初期化エンドポイント
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
		h.DB.Select(&unreads, "SELECT * FROM `unread_announcements`")
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
		rdb1.FlushDB(ctx)
	}
	{
		rdb2.FlushDB(ctx)
	}
	// アプリ複数台のときは初期化されないことがないか気をつけること
	sessionCache = sync.Map{}
	res := InitializeResponse{
		Language: "go",
	}
	time.Sleep(15 * time.Second)
	return c.JSON(http.StatusOK, res)
}

// IsLoggedIn ログイン確認用middleware
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

// IsAdmin admin確認用middleware
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

type CourseType string

const (
	LiberalArts   CourseType = "liberal-arts"
	MajorSubjects CourseType = "major-subjects"
)

type DayOfWeek string

const (
	Monday    DayOfWeek = "monday"
	Tuesday   DayOfWeek = "tuesday"
	Wednesday DayOfWeek = "wednesday"
	Thursday  DayOfWeek = "thursday"
	Friday    DayOfWeek = "friday"
)

var daysOfWeek = []DayOfWeek{Monday, Tuesday, Wednesday, Thursday, Friday}

type CourseStatus string

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

// Login POST /login ログイン
func (h *handlers) Login(c echo.Context) error {
	var req LoginRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}

	var user User
	if err := h.Replica.Get(&user, "SELECT * FROM `users` WHERE `code` = ?", req.Code); err != nil && err != sql.ErrNoRows {
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

// Logout POST /logout ログアウト
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

// GetMe GET /api/users/me 自身の情報を取得
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

// GetRegisteredCourses GET /api/users/me/courses 履修中の科目一覧取得
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

	// 履修科目が0件の時は空配列を返却
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

// RegisterCourses PUT /api/users/me/courses 履修登録
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

		// すでに履修登録済みの科目は無視する
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
	GpaTScore float64 `json:"gpa_t_score"` // 偏差値
	GpaAvg    float64 `json:"gpa_avg"`     // 平均値
	GpaMax    float64 `json:"gpa_max"`     // 最大値
	GpaMin    float64 `json:"gpa_min"`     // 最小値
}

type CourseResult struct {
	Name             string       `json:"name"`
	Code             string       `json:"code"`
	TotalScore       int          `json:"total_score"`
	TotalScoreTScore float64      `json:"total_score_t_score"` // 偏差値
	TotalScoreAvg    float64      `json:"total_score_avg"`     // 平均値
	TotalScoreMax    int          `json:"total_score_max"`     // 最大値
	TotalScoreMin    int          `json:"total_score_min"`     // 最小値
	ClassScores      []ClassScore `json:"class_scores"`
}

type ClassScore struct {
	ClassID    string `json:"class_id"`
	Title      string `json:"title"`
	Part       uint8  `json:"part"`
	Score      *int   `json:"score"`      // 0~100点
	Submitters int    `json:"submitters"` // 提出した学生数
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

// GetGrades GET /api/users/me/grades 成績取得
func (h *handlers) GetGrades(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// 履修している科目一覧取得
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

	// 科目毎の成績計算処理
	myGPA := 0.0
	myCredits := 0
	courseDict := make(map[string]CourseResultWithMyTotalScore)
	t1 := time.Now()
	for _, classWithCourse := range registeredClasses {
		// 講義毎の成績計算処理
		classScores, ok := courseDict[classWithCourse.Course.ID]
		if !ok {
			classScores = CourseResultWithMyTotalScore{make([]ClassScore, 0), Course{}, 0}
		}
		var submissionsCount int
		if classWithCourse.Class.ID != nil {
			// TODO: N+1 (gnu)
			if err := h.DB.Get(&submissionsCount, "SELECT COUNT(*) FROM `submissions` WHERE `class_id` = ?", classWithCourse.Class.ID); err != nil {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			}

			var myScore sql.NullInt64
			// TODO: N+1 (gnu)
			if err := h.DB.Get(&myScore, "SELECT `submissions`.`score` FROM `submissions` WHERE `user_id` = ? AND `class_id` = ?", userID, classWithCourse.Class.ID); err != nil && err != sql.ErrNoRows {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			} else if err == sql.ErrNoRows || !myScore.Valid {
				classScores.classScores = append(classScores.classScores, ClassScore{
					ClassID:    *classWithCourse.Class.ID,
					Part:       *classWithCourse.Class.Part,
					Title:      *classWithCourse.Class.Title,
					Score:      nil,
					Submitters: submissionsCount,
				})
			} else {
				score := int(myScore.Int64)
				classScores.myTotalScore += score
				classScores.classScores = append(classScores.classScores, ClassScore{
					ClassID:    *classWithCourse.Class.ID,
					Part:       *classWithCourse.Class.Part,
					Title:      *classWithCourse.Class.Title,
					Score:      &score,
					Submitters: submissionsCount,
				})
			}
		}
		classScores.Course = classWithCourse.Course
		courseDict[classWithCourse.Course.ID] = classScores
	}
	t2 := time.Now()
	courseResults := make([]CourseResult, 0, len(registeredClasses))
	for _, res := range courseDict {
		// この科目を履修している学生のTotalScore一覧を取得
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

		// 自分のGPA計算
		if res.Course.Status == StatusClosed {
			myGPA += float64(res.myTotalScore * int(res.Course.Credit))
			myCredits += int(res.Course.Credit)
		}
	}
	t3 := time.Now()
	if myCredits > 0 {
		myGPA = myGPA / 100 / float64(myCredits)
	}

	// GPAの統計値
	// 一つでも修了した科目がある学生のGPA一覧
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

// SearchCourses GET /api/courses 科目検索
func (h *handlers) SearchCourses(c echo.Context) error {
	query := "SELECT `courses`.*, `users`.`name` AS `teacher`" +
		" FROM `courses` JOIN `users` ON `courses`.`teacher_id` = `users`.`id`" +
		" WHERE 1=1"
	var condition string
	var args []interface{}

	// 無効な検索条件はエラーを返さず無視して良い

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

	// limitより多く上限を設定し、実際にlimitより多くレコードが取得できた場合は次のページが存在する
	condition += " LIMIT ? OFFSET ?"
	args = append(args, limit+1, offset)

	// 結果が0件の時は空配列を返却
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

// AddCourse POST /api/courses 新規科目登録
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
			return c.JSON(http.StatusCreated, AddCourseResponse{ID: course.ID})
		}
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	rdb1.Set(c.Request().Context(), courseID, StatusRegistration, 0).Result()

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

// GetCourseDetail GET /api/courses/:courseID 科目詳細の取得
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

// SetCourseStatus PUT /api/courses/:courseID/status 科目のステータスを変更
func (h *handlers) SetCourseStatus(c echo.Context) error {
	courseID := c.Param("courseID")

	var req SetCourseStatusRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}
	// coursesはinsertのみなのでTransactionは不要
	var count int
	if err := h.DB.Get(&count, "SELECT COUNT(*) FROM `courses` WHERE `id` = ?", courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if count == 0 {
		return c.String(http.StatusNotFound, "No such course.")
	}

	if _, err := h.DB.Exec("UPDATE `courses` SET `status` = ? WHERE `id` = ?", req.Status, courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if _, err := rdb1.Set(c.Request().Context(), courseID, string(req.Status), 0).Result(); err != nil {
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

// GetClasses GET /api/courses/:courseID/classes 科目に紐づく講義一覧の取得
func (h *handlers) GetClasses(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseID := c.Param("courseID")
	// courses は増えることはあっても消えることはないのでTransactionは不要
	var count int
	if err := h.DB.Get(&count, "SELECT COUNT(*) FROM `courses` WHERE `id` = ?", courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if count == 0 {
		return c.String(http.StatusNotFound, "No such course.")
	}

	var classes []ClassWithSubmitted
	query := "SELECT `classes`.*, `submissions`.`user_id` IS NOT NULL AS `submitted`" +
		" FROM `classes`" +
		" LEFT JOIN `submissions` ON `classes`.`id` = `submissions`.`class_id` AND `submissions`.`user_id` = ?" +
		" WHERE `classes`.`course_id` = ?" +
		" ORDER BY `classes`.`part`"
	if err := h.DB.Select(&classes, query, userID, courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// 結果が0件の時は空配列を返却
	res := make([]GetClassResponse, 0, len(classes))
	for _, class := range classes {
		res = append(res, GetClassResponse{
			ID:               class.ID,
			Part:             class.Part,
			Title:            class.Title,
			Description:      class.Description,
			SubmissionClosed: class.SubmissionClosed,
			Submitted:        class.Submitted,
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

// AddClass POST /api/courses/:courseID/classes 新規講義(&課題)追加
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

// SubmitAssignment POST /api/courses/:courseID/classes/:classID/assignments 課題の提出
func (h *handlers) SubmitAssignment(c echo.Context) error {
	userID, _, _, userCode, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	courseID := c.Param("courseID")
	classID := c.Param("classID")

	tx, err := h.DB.Beginx()
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	status, err := rdb1.Get(c.Request().Context(), courseID).Result()
	if err != nil {
		if err == redis.Nil {
			var status CourseStatus
			if err := tx.Get(&status, "SELECT `status` FROM `courses` WHERE `id` = ? FOR SHARE", courseID); err != nil && err != sql.ErrNoRows {
				c.Logger().Error(err)
				return c.NoContent(http.StatusInternalServerError)
			} else if err == sql.ErrNoRows {
				return c.String(http.StatusNotFound, "No such course.")
			}
			_, err = rdb1.Set(c.Request().Context(), courseID, StatusInProgress, 0).Result()
			if status != StatusInProgress {
				return c.String(http.StatusBadRequest, "This course is not in progress.")
			}
			if err != nil {
				c.Logger().Error(err)
			}
		}
		return c.NoContent(http.StatusInternalServerError)
	} else {
		if status != string(StatusInProgress) {
			return c.String(http.StatusBadRequest, "This course is not in progress.")
		}
	}

	// 科目を履修してるか
	var registrationCount int
	if err := tx.Get(&registrationCount, "SELECT COUNT(*) FROM `registrations` WHERE `user_id` = ? AND `course_id` = ?", userID, courseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if registrationCount == 0 {
		return c.String(http.StatusBadRequest, "You have not taken this course.")
	}

	submissionClosedString, err := rdb2.Get(c.Request().Context(), classID).Result()
	if err != nil {
		if err == redis.Nil {
			var submissionClosed bool
			if err := tx.Get(&submissionClosed, "SELECT `submission_closed` FROM `classes` WHERE `id` = ? FOR SHARE", classID); err != nil && err != sql.ErrNoRows {
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

	file, header, err := c.Request().FormFile("file")
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid file.")
	}
	defer file.Close()

	if _, err := tx.Exec("INSERT INTO `submissions` (`user_id`, `user_code`, `class_id`, `file_name`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `file_name` = VALUES(`file_name`)", userID, userCode, classID, header.Filename); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	data, err := myReadAll(file)

	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if err := tx.Commit(); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	go func() {
		dst := AssignmentsDirectory + classID + "-" + userID + ".pdf"
		if err := os.WriteFile(dst, data, 0666); err != nil {
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

// RegisterScores PUT /api/courses/:courseID/classes/:classID/assignments/scores 採点結果登録
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

	// for _, score := range req {
	// 	// TODO: N+1 (gnu)
	// 	if _, err := tx.Exec("UPDATE `submissions` SET `score` = ? WHERE `user_code` = ? AND `class_id` = ?", score.Score, score.UserCode, classID); err != nil {
	// 		c.Logger().Error(err)
	// 		return c.NoContent(http.StatusInternalServerError)
	// 	}
	// }

	return c.NoContent(http.StatusNoContent)
}

type Submission struct {
	UserID   string `db:"user_id"`
	UserCode string `db:"user_code"`
	FileName string `db:"file_name"`
}

// DownloadSubmittedAssignments GET /api/courses/:courseID/classes/:classID/assignments/export 提出済みの課題ファイルをzip形式で一括ダウンロード
func (h *handlers) DownloadSubmittedAssignments(c echo.Context) error {
	classID := c.Param("classID")
	// クラスは増えるのみなのでtx不要
	var classCount int
	if err := h.DB.Get(&classCount, "SELECT COUNT(*) FROM `classes` WHERE `id` = ?", classID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if classCount == 0 {
		return c.String(http.StatusNotFound, "No such class.")
	}

	// submission_closed かどうかを観ていないのでtx不要
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

	// ファイル名を指定の形式に変更
	// for _, submission := range submissions {
	// 	src := AssignmentsDirectory + classID + "-" + submission.UserID + ".pdf"
	// 	dst := tmpDir + submission.UserCode + "-" + submission.FileName
	// 	os.Symlink(dst, src)
	// }
	// for _, submission := range submissions {
	// 	// emulate cp
	// 	srcName := AssignmentsDirectory + classID + "-" + submission.UserID + ".pdf"
	// 	dstName := tmpDir + submission.UserCode + "-" + submission.FileName
	// 	src, err := os.Open(srcName)
	// 	if err != nil {
	// 		continue
	// 	}
	// 	defer src.Close()
	// 	dst, err := os.Create(dstName)
	// 	if err != nil {
	// 		continue
	// 	}
	// 	defer dst.Close()
	// 	io.Copy(dst, src)
	// }
	// ファイル名を指定の形式に変更
	for _, submission := range submissions {
		if err := exec.Command(
			"cp",
			AssignmentsDirectory+classID+"-"+submission.UserID+".pdf",
			tmpDir+submission.UserCode+"-"+submission.FileName,
		).Run(); err != nil {
			return err
		}
	}

	// -i 'tmpDir/*': 空zipを許す
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

// GetAnnouncementList GET /api/announcements お知らせ一覧取得
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
	// limitより多く上限を設定し、実際にlimitより多くレコードが取得できた場合は次のページが存在する
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
		// 通常は未読ではない
		// redisにあったら未読
		_, unread := unreadMap[announcements[i].ID]
		announcements[i].Unread = unread
	}

	// 対象になっているお知らせが0件の時は空配列を返却
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

// AddAnnouncement POST /api/announcements 新規お知らせ追加
func (h *handlers) AddAnnouncement(c echo.Context) error {
	var req AddAnnouncementRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "Invalid format.")
	}
	// coursesは増えるのみなのでtx不要
	var count int
	if err := h.DB.Get(&count, "SELECT COUNT(*) FROM `courses` WHERE `id` = ?", req.CourseID); err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}
	if count == 0 {
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

// GetAnnouncementDetail GET /api/announcements/:announcementID お知らせ詳細取得
func (h *handlers) GetAnnouncementDetail(c echo.Context) error {
	userID, _, _, _, err := getUserInfo(c)
	if err != nil {
		c.Logger().Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	announcementID := c.Param("announcementID")

	// is_deletedにするだけなのでトランザクションは不要
	// unread_announcements はinsertのみ
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
