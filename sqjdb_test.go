package sqjdb_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/daaku/ensure"
	"github.com/daaku/sqjdb"
	"github.com/oklog/ulid/v2"
	"zombiezen.com/go/sqlite"
)

type Jedi struct {
	ID   string
	Name string
	Age  int
}

var (
	yoda = Jedi{
		ID:   ulid.Make().String(),
		Name: "yoda",
		Age:  980,
	}
	luke = Jedi{
		ID:   ulid.Make().String(),
		Name: "luke",
		Age:  42,
	}
	leia = Jedi{
		ID:   ulid.Make().String(),
		Name: "leia",
		Age:  42,
	}
)

var jedis = sqjdb.NewTable[Jedi]("jedis")

func newConn(t *testing.T) *sqlite.Conn {
	mode := "mode=memory&"
	if os.Getenv("NO_MEMORY") == "1" {
		mode = ""
	}
	conn, err := sqlite.OpenConn(fmt.Sprintf("file:%s.db?%scache=shared", t.Name(), mode))
	ensure.Nil(t, err)
	ensure.Nil(t, jedis.Migrate(conn))
	_, err = jedis.Insert(conn, &yoda)
	ensure.Nil(t, err)
	_, err = jedis.Insert(conn, &luke)
	ensure.Nil(t, err)
	_, err = jedis.Insert(conn, &leia)
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

func TestMigrateIDIndex(t *testing.T) {
	conn := newConn(t)
	stmt := conn.Prep(`explain select data from jedis where data->>'ID' = 'a'`)
	for {
		hasRow, err := stmt.Step()
		ensure.Nil(t, err)
		if !hasRow {
			t.Fatal("missing expected opcode")
		}
		if stmt.GetText("opcode") == "IdxGT" {
			break
		}
	}
}

func TestOne(t *testing.T) {
	conn := newConn(t)
	yodaFetched, err := jedis.One(conn, sqjdb.ByID(yoda.ID))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, yoda.Name, yodaFetched.Name)
}

func byAge(age int) sqjdb.SQL {
	return sqjdb.SQL{Query: "where data->>'Age' = ?", Args: []any{age}}
}

func TestAll(t *testing.T) {
	conn := newConn(t)
	rows42, err := jedis.All(conn, byAge(luke.Age))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(rows42), 2)
	rowsYoda, err := jedis.All(conn, byAge(yoda.Age))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(rowsYoda), 1)
}

func TestDelete(t *testing.T) {
	conn := newConn(t)
	beforeDelete, err := jedis.All(conn, byAge(luke.Age))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(beforeDelete), 2)
	err = jedis.Delete(conn, sqjdb.ByID(luke.ID))
	ensure.Nil(t, err)
	afterDelete, err := jedis.All(conn, byAge(luke.Age))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(afterDelete), 1)
}
