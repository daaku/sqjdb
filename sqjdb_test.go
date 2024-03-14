package sqjdb_test

import (
	"fmt"
	"strings"
	"sync/atomic"
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

var yoda = Jedi{
	ID:   ulid.Make().String(),
	Name: "yoda",
	Age:  980,
}

var large = Jedi{
	ID:   ulid.Make().String(),
	Name: strings.Repeat("yoda", 1000),
	Age:  981,
}

var (
	jedis   = sqjdb.NewTable[Jedi]("jedis")
	counter atomic.Int32
)

func newConn(t testing.TB) *sqlite.Conn {
	conn, err := sqlite.OpenConn(fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), counter.Add(1)))
	ensure.Nil(t, err)
	ensure.Nil(t, jedis.Migrate(conn))
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

func TestCRUD(t *testing.T) {
	conn := newConn(t)
	_, err := jedis.Insert(conn, &yoda)
	ensure.Nil(t, err)
	yodaFetched, err := jedis.One(conn, sqjdb.ByID(yoda.ID))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, yoda.Name, yodaFetched.Name)
}

func prepOne(b *testing.B) *sqlite.Conn {
	conn := newConn(b)
	_, err := jedis.Insert(conn, &yoda)
	ensure.Nil(b, err)
	_, err = jedis.Insert(conn, &large)
	ensure.Nil(b, err)
	return conn
}

func BenchmarkOneText(b *testing.B) {
	conn := prepOne(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		y, err := jedis.One(conn, sqjdb.ByID(yoda.ID))
		if err != nil {
			b.Fatal("unexpected")
		}
		if y.Age != yoda.Age {
			b.Fatal("unexpected")
		}

		l, err := jedis.One(conn, sqjdb.ByID(large.ID))
		if err != nil {
			b.Fatal("unexpected")
		}
		if l.Age != large.Age {
			b.Fatal("unexpected")
		}
	}
}

func BenchmarkOneReader(b *testing.B) {
	conn := prepOne(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		y, err := jedis.OneReader(conn, sqjdb.ByID(yoda.ID))
		if err != nil {
			b.Fatal("unexpected")
		}
		if y.Age != yoda.Age {
			b.Fatal("unexpected")
		}

		l, err := jedis.OneReader(conn, sqjdb.ByID(large.ID))
		if err != nil {
			b.Fatal("unexpected")
		}
		if l.Age != large.Age {
			b.Fatal("unexpected")
		}
	}
}
