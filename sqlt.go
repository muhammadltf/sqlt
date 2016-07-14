package sqlt

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"database/sql"

	"github.com/jmoiron/sqlx"
)

//DB struct wrapper for sqlx connection
type DB struct {
	sqlxdb     []sqltdb
	activeDB   []sqltdb
	pendingDB  []sqltdb
	driverName string
	groupName  string
	length     int
	count      uint64
	//for stats
	heartbeat bool
	lastBeat  string
}

type sqltdb struct {
	Database   *sqlx.DB    `json:"database address"`
	Name       string      `json:"name"`
	Connected  bool        `json:"connected"`
	LastActive string      `json:"last_active"`
	Error      interface{} `json:"error"`
}

type statusResponse struct {
	Dbs       interface{} `json:"db_list"`
	Heartbeat bool        `json:"heartbeat"`
	Lastbeat  string      `json:"last_beat"`
}

const defaultGroupName = "sqlt_open"

func openConnection(driverName, sources string, groupName string) (*DB, error) {
	var err error

	conns := strings.Split(sources, ";")
	connsLength := len(conns)

	//check if no source is available
	if connsLength < 1 {
		return nil, errors.New("No sources found")
	}

	db := &DB{
		sqlxdb: make([]sqltdb, connsLength),
	}
	db.length = connsLength
	db.driverName = driverName

	for i := range conns {
		db.sqlxdb[i].Database, err = sqlx.Open(driverName, conns[i])

		if err != nil {
			db.pendingDB = append(db.pendingDB, db.sqlxdb[i])
			return nil, err
		}

		constatus := true

		//set the name
		name := ""
		if i == 0 {
			name = "master"
		} else {
			name = "slave-" + strconv.Itoa(i)
		}

		db.sqlxdb[i].Name = name
		db.sqlxdb[i].Connected = constatus
		db.sqlxdb[i].LastActive = time.Now().String()
		db.activeDB = append(db.activeDB, db.sqlxdb[i])
	}

	//set the default group name
	db.groupName = defaultGroupName
	if groupName != "" {
		db.groupName = groupName
	}

	//ping database to retrieve error
	err = db.Ping()
	return db, err
}

//Open connection to database
func Open(driverName, sources string) (*DB, error) {
	return openConnection(driverName, sources, "")
}

//OpenWithName open the connection and set connection group name
func OpenWithName(driverName, sources string, name string) (*DB, error) {
	return openConnection(driverName, sources, name)
}

//GetStatus return database status
func (db *DB) GetStatus() ([]sqltdb, error) {
	if len(db.sqlxdb) == 0 {
		return db.sqlxdb, errors.New("No connection detected")
	}

	//if heartbeat is not enabled, ping to get status before send status
	if !db.heartbeat {
		db.Ping()
	}

	return db.sqlxdb, nil
}

//GetJSONStatus return status of database in JSON string
func (db *DB) GetJSONStatus() (string, error) {
	if len(db.sqlxdb) == 0 {
		return "", errors.New("No connection detected")
	}

	response := make(map[string]interface{})

	status := statusResponse{
		Dbs:       append(db.activeDB, db.pendingDB...),
		Heartbeat: db.heartbeat,
		Lastbeat:  db.lastBeat,
	}

	response[db.groupName] = status

	jsonByte, err := json.Marshal(response)

	if err != nil {
		return "", err
	}

	return string(jsonByte), nil
}

//DoHeartBeat will automatically spawn a goroutines to ping your database every one second, use this carefully
func (db *DB) DoHeartBeat() {
	if !db.heartbeat {
		go func() {
			for range time.Tick(time.Second * 1) {
				db.Ping()
				db.lastBeat = time.Now().Format(time.RFC1123)
			}
		}()
	}

	db.heartbeat = true
}

//Ping database
//using activedb and pendingdb method
func (db *DB) Ping() error {
	var err error

	for i := 0; i < len(db.activeDB); {
		err = db.activeDB[i].Database.Ping()
		name := db.activeDB[i].Name

		if err != nil {
			db.activeDB[i].Connected = false
			db.activeDB[i].Error = errors.New(name + ": " + err.Error())

			db.pendingDB = append(db.pendingDB, db.activeDB[i])
			db.activeDB = append(db.activeDB[:i], db.activeDB[i+1:]...)
		} else {
			//only update lastactive
			db.activeDB[i].LastActive = time.Now().String()

			i++
		}
	}

	for i := 0; i < len(db.pendingDB); {
		err = db.pendingDB[i].Database.Ping()
		name := db.pendingDB[i].Name

		if err == nil {
			db.pendingDB[i].Connected = true
			db.pendingDB[i].LastActive = time.Now().String()
			db.pendingDB[i].Error = nil

			db.activeDB = append(db.activeDB, db.pendingDB[i])
			db.pendingDB = append(db.pendingDB[:i], db.pendingDB[i+1:]...)
		} else {
			//only update error
			db.activeDB[i].Error = errors.New(name + ": " + err.Error())
			i++
		}
	}

	db.length = len(db.activeDB)
	db.sqlxdb = db.activeDB
	return err
}

//Preare will return sql stmt
func (db *DB) Prepare(query string) (Stmt, error) {
	var err error
	stmt := Stmt{}
	stmts := make([]*sql.Stmt, db.length)

	for i := range db.sqlxdb {
		stmts[i], err = db.sqlxdb[i].Database.Prepare(query)

		if err != nil {
			return stmt, err
		}
	}

	stmt.db = db
	stmt.stmts = stmts
	return stmt, nil
}

//Preparex sqlx stmt
func (db *DB) Preparex(query string) (*Stmtx, error) {
	var err error
	stmts := make([]*sqlx.Stmt, db.length)

	for i := range db.sqlxdb {
		stmts[i], err = db.sqlxdb[i].Database.Preparex(query)

		if err != nil {
			return nil, err
		}
	}

	return &Stmtx{db: db, stmts: stmts}, nil
}

func (db *DB) SetMaxOpenConnections(max int) {
	for i := range db.sqlxdb {
		db.sqlxdb[i].Database.SetMaxOpenConns(max)
	}
}

//Slave return slave database
func (db *DB) Slave() *DB {
	slavedb := &DB{sqlxdb: make([]sqltdb, 1)}
	slavedb.sqlxdb[0] = db.sqlxdb[db.slave()]

	return slavedb
}

//Master return master database
func (db *DB) Master() *DB {
	masterdb := &DB{sqlxdb: make([]sqltdb, 1)}
	masterdb.sqlxdb[0] = db.sqlxdb[0]

	return masterdb
}

// Queryx queries the database and returns an *sqlx.Rows.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	r, err := db.sqlxdb[db.slave()].Database.Query(query, args...)
	return r, err
}

// QueryRowx queries the database and returns an *sqlx.Row.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	rows := db.sqlxdb[db.slave()].Database.QueryRow(query, args...)
	return rows
}

// Queryx queries the database and returns an *sqlx.Rows.
func (db *DB) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	r, err := db.sqlxdb[db.slave()].Database.Queryx(query, args...)
	return r, err
}

// QueryRowx queries the database and returns an *sqlx.Row.
func (db *DB) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	rows := db.sqlxdb[db.slave()].Database.QueryRowx(query, args...)
	return rows
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.sqlxdb[0].Database.Exec(query, args...)
}

//Using master db
// MustExec (panic) runs MustExec using this database.
func (db *DB) MustExec(query string, args ...interface{}) sql.Result {
	return db.sqlxdb[0].Database.MustExec(query, args...)
}

// Select using this DB.
func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {
	return db.sqlxdb[db.slave()].Database.Select(dest, query, args...)
}

// Get using this DB.
func (db *DB) Get(dest interface{}, query string, args ...interface{}) error {
	return db.sqlxdb[db.slave()].Database.Get(dest, query, args...)
}

//Using master db
// NamedExec using this DB.
func (db *DB) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return db.sqlxdb[0].Database.NamedExec(query, arg)
}

//Using master db
// MustBegin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (db *DB) MustBegin() *sqlx.Tx {
	tx, err := db.sqlxdb[0].Database.Beginx()
	if err != nil {
		panic(err)
	}
	return tx
}

/*******************************************/

//Stmt implement sql stmt
type Stmt struct {
	db    *DB
	stmts []*sql.Stmt
}

//Exec will always go to production
func (st *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	return st.stmts[0].Exec(args...)
}

//Query will always go to slave
func (st *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return st.stmts[st.db.slave()].Query(args...)
}

//QueryRow will always go to slave
func (st *Stmt) QueryRow(args ...interface{}) *sql.Row {
	return st.stmts[st.db.slave()].QueryRow(args...)
}

//Close stmt
func (st *Stmt) Close() error {
	for i := range st.stmts {
		err := st.stmts[i].Close()

		if err != nil {
			return err
		}
	}

	return nil
}

/********************************************/

//Stmtx implement sqlx stmt
type Stmtx struct {
	db    *DB
	stmts []*sqlx.Stmt
}

func (st *Stmtx) Close() error {
	for i := range st.stmts {
		err := st.stmts[i].Close()

		if err != nil {
			return err
		}
	}

	return nil
}

//Exec will always go to production
func (st *Stmtx) Exec(args ...interface{}) (sql.Result, error) {
	return st.stmts[0].Exec(args...)

}

//Query will always go to slave
func (st *Stmtx) Query(args ...interface{}) (*sql.Rows, error) {
	return st.stmts[st.db.slave()].Query(args...)
}

//QueryRow will always go to slave
func (st *Stmtx) QueryRow(args ...interface{}) *sql.Row {
	return st.stmts[st.db.slave()].QueryRow(args...)
}

func (st *Stmtx) MustExec(args ...interface{}) sql.Result {
	return st.stmts[0].MustExec(args...)
}

//Query will always go to slave
func (st *Stmtx) Queryx(args ...interface{}) (*sqlx.Rows, error) {
	return st.stmts[st.db.slave()].Queryx(args...)
}

//QueryRow will always go to slave
func (st *Stmtx) QueryRowx(args ...interface{}) *sqlx.Row {
	return st.stmts[st.db.slave()].QueryRowx(args...)
}

//Query will always go to slave
func (st *Stmtx) Get(dest interface{}, args ...interface{}) error {
	return st.stmts[st.db.slave()].Get(dest, args...)
}

//QueryRow will always go to slave
func (st *Stmtx) Select(dest interface{}, args ...interface{}) error {
	return st.stmts[st.db.slave()].Select(dest, args...)
}

//slave
func (db *DB) slave() int {
	if db.length <= 1 {
		return 0
	}

	slave := int(1 + (atomic.AddUint64(&db.count, 1) % uint64(db.length-1)))

	return slave
}
