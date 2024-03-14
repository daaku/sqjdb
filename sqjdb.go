package sqjdb

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"

	"braces.dev/errtrace"
	"github.com/oklog/ulid/v2"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var ErrNoRows = errors.New("sqjson: no rows in result set")

func Bind(stmt *sqlite.Stmt, i int, v any) error {
	switch v := v.(type) {
	default:
		return errtrace.Errorf("unexpected value %v of type %T", v, v)
	case int:
		stmt.BindInt64(i, int64(v))
	case int16:
		stmt.BindInt64(i, int64(v))
	case int32:
		stmt.BindInt64(i, int64(v))
	case int64:
		stmt.BindInt64(i, int64(v))
	case bool:
		stmt.BindBool(i, v)
	case []byte:
		stmt.BindBytes(i, v)
	case float32:
		stmt.BindFloat(i, float64(v))
	case float64:
		stmt.BindFloat(i, v)
	case nil:
		stmt.BindNull(i)
	case string:
		stmt.BindText(i, v)
	}
	return nil
}

type SQL struct {
	Query string
	Args  []any
}

func ByID(id string) SQL {
	return SQL{Query: "where data->>'ID' = ?", Args: []any{id}}
}

type Table[T any] struct {
	Name    string
	qInsert string
}

func NewTable[T any](name string) Table[T] {
	return Table[T]{
		Name:    name,
		qInsert: "insert into " + name + " (data) values (jsonb(?))",
	}
}

func (t *Table[T]) Migrate(conn *sqlite.Conn) error {
	qCreate := "create table if not exists " + t.Name + " (data blob)"
	if err := sqlitex.ExecuteTransient(conn, qCreate, nil); err != nil {
		return errtrace.Errorf("creating table %q: %w", t.Name, err)
	}
	qIndexID := "create unique index if not exists " + t.Name +
		"_ID on " + t.Name + " (data->>'ID')"
	if err := sqlitex.ExecuteTransient(conn, qIndexID, nil); err != nil {
		return errtrace.Errorf("creating ID index on %q: %w", t.Name, err)
	}
	return nil
}

func (t *Table[T]) Insert(conn *sqlite.Conn, doc *T) (*T, error) {
	reflectV := reflect.Indirect(reflect.ValueOf(doc))
	vID := reflectV.FieldByName("ID")
	if !vID.IsValid() {
		return nil, errtrace.Errorf("expected type %T to contain an ID field of type string", doc)
	}
	if vID.IsZero() {
		docCopy := *doc
		doc = &docCopy
		reflect.Indirect(reflect.ValueOf(doc)).FieldByName("ID").SetString(ulid.Make().String())
	}
	jsonS, err := json.Marshal(doc)
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	stmt, err := conn.Prepare(t.qInsert)
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	stmt.BindText(1, string(jsonS))
	if _, err := stmt.Step(); err != nil {
		return nil, errtrace.Errorf("inserting document in %q: %w", t.Name, err)
	}
	return doc, nil
}

func addSQLQuery(query *strings.Builder, sqls []SQL) {
	for _, part := range sqls {
		query.WriteRune(' ')
		query.WriteString(part.Query)
	}
}

func bindSQLQuery(stmt *sqlite.Stmt, sqls []SQL) error {
	i := 1 // Bind Parameter indices start at 1.
	for _, part := range sqls {
		for _, arg := range part.Args {
			if err := Bind(stmt, i, arg); err != nil {
				return errtrace.Wrap(err)
			}
			i++
		}
	}
	return nil
}

func (t *Table[T]) stepOne(stmt *sqlite.Stmt) (*T, error) {
	rowReturned, err := stmt.Step()
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	if !rowReturned {
		return nil, nil
	}
	jsonS := stmt.ColumnText(0)
	v := new(T)
	if err := json.Unmarshal([]byte(jsonS), v); err != nil {
		return nil, errtrace.Errorf("invalid json from db: %w\n%s", err, jsonS)
	}
	return v, nil
}

func (t *Table[T]) One(conn *sqlite.Conn, sqls ...SQL) (*T, error) {
	var query strings.Builder
	query.WriteString("select json(data) from ")
	query.WriteString(t.Name)
	addSQLQuery(&query, sqls)
	query.WriteString(" limit 1")
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return nil, err
	}
	v, err := t.stepOne(stmt)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errtrace.Wrap(ErrNoRows)
	}
	return v, nil
}

func (t *Table[T]) All(conn *sqlite.Conn, sqls ...SQL) ([]*T, error) {
	var query strings.Builder
	query.WriteString("select json(data) from ")
	query.WriteString(t.Name)
	addSQLQuery(&query, sqls)
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return nil, err
	}
	var rows []*T
	for {
		v, err := t.stepOne(stmt)
		if err != nil {
			return nil, err
		}
		if v == nil {
			break
		}
		rows = append(rows, v)
	}
	return rows, nil
}

func (t *Table[T]) Delete(conn *sqlite.Conn, sqls ...SQL) error {
	var query strings.Builder
	query.WriteString("delete from ")
	query.WriteString(t.Name)
	addSQLQuery(&query, sqls)
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return errtrace.Wrap(err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return errtrace.Wrap(err)
	}
	return nil
}

func (t *Table[T]) patchOrReplace(partQ string, conn *sqlite.Conn, doc *T, sqls []SQL) error {
	var query strings.Builder
	query.WriteString("update ")
	query.WriteString(t.Name)
	jsonS, err := json.Marshal(doc)
	if err != nil {
		return errtrace.Wrap(err)
	}
	sqls = slices.Concat([]SQL{{Query: partQ, Args: []any{jsonS}}}, sqls)
	addSQLQuery(&query, sqls)
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return errtrace.Wrap(err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return errtrace.Wrap(err)
	}
	return nil
}

func (t *Table[T]) Patch(conn *sqlite.Conn, doc *T, sqls ...SQL) error {
	return t.patchOrReplace("set data = jsonb_patch(data, ?)", conn, doc, sqls)
}

func (t *Table[T]) Replace(conn *sqlite.Conn, doc *T, sqls ...SQL) error {
	return t.patchOrReplace("set data = jsonb(?)", conn, doc, sqls)
}
