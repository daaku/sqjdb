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

func TestCRUD(t *testing.T) {
	conn := newConn(t)
	defer conn.Close()
	_, err := jedis.Insert(conn, &yoda)
	ensure.Nil(t, err)
	yodaFetched, err := jedis.One(conn, sqjdb.SQL{Query: "where data->>'ID' = ?", Args: []any{yoda.ID}})
	ensure.Nil(t, err)
	ensure.DeepEqual(t, yoda.Name, yodaFetched.Name)
}
