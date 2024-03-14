package sqjdb_test

import (
	"fmt"
	"testing"

	"github.com/daaku/ensure"
	"github.com/daaku/sqjdb"
	"github.com/oklog/ulid/v2"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Jedi struct {
	ID   string
	Name string
	Age  int
}

var yoda = Jedi{
	ID:   ulid.Make().String(),
	Name: "yoda",
	Age:  980,
}

var jedis = sqjdb.NewTable[Jedi]("jedis")

func newConn(t *testing.T) *sqlite.Conn {
	conn, err := sqlite.OpenConn(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	ensure.Nil(t, err)
	err = sqlitex.ExecuteTransient(conn, "create table jedis (data blob)", nil)
	ensure.Nil(t, err)
	return conn
}

func TestIDIsGenerated(t *testing.T) {
	conn := newConn(t)
	yodaToInsert := &Jedi{Name: yoda.Name}
	yodaInserted, err := jedis.Insert(conn, yodaToInsert)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, yodaToInsert.ID, "")
	ensure.NotDeepEqual(t, len(yodaInserted.ID), 0)
	yodaFetched, err := jedis.One(conn, sqjdb.ByID(yodaInserted.ID))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, yodaInserted.Name, yodaFetched.Name)
}

func TestCRUD(t *testing.T) {
	conn := newConn(t)
	_, err := jedis.Insert(conn, &yoda)
	ensure.Nil(t, err)
	yodaFetched, err := jedis.One(conn, sqjdb.ByID(yoda.ID))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, yoda.Name, yodaFetched.Name)
}
