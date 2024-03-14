package sqjdb

import (
	"encoding/json"
	"reflect"
	"strings"

	"braces.dev/errtrace"
	"github.com/oklog/ulid/v2"
	"zombiezen.com/go/sqlite"
)

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

func (t *Table[T]) One(conn *sqlite.Conn, sqls ...SQL) (*T, error) {
	var query strings.Builder
	query.WriteString("select json(data) from ")
	query.WriteString(t.Name)
	for _, part := range sqls {
		query.WriteRune(' ')
		query.WriteString(part.Query)
	}
	query.WriteString(" limit 1")
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	i := 1 // Bind Parameter indices start at 1.
	for _, part := range sqls {
		for _, arg := range part.Args {
			if err := Bind(stmt, i, arg); err != nil {
				return nil, errtrace.Wrap(err)
			}
			i++
		}
	}
	rowReturned, err := stmt.Step()
	if err != nil {
		return nil, errtrace.Wrap(err)
	}
	if !rowReturned {
		return nil, nil
	}
	// TODO: benchmark if this is better than ColumnReader
	jsonS := stmt.ColumnText(0)
	v := new(T)
	if err := json.Unmarshal([]byte(jsonS), v); err != nil {
		return nil, errtrace.Errorf("invalid json from db: %w\n%s", err, jsonS)
	}
	return v, nil
}
