// Package sqjdb provides an opinionated libary to store JSON encoded documents
// in a SQLite Database.
//
// It has various opinions about how you go about doing this:
//  1. Documents contain an "ID" field of type string. You can manage it, or it
//     will be filled in for you with ULIDs.
//  2. Tables store the JSON document in a column named "data". It's JSONB.
//  3. SQL is is only lightly hidden from you.
//  4. Make indexes on your document fields. The standard migrations, if you use
//     them, will make one on the ID field for you.
package sqjdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/oklog/ulid/v2"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// ErrNoDoc indicates the requested query did not match a document.
var ErrNoDoc = errors.New("sqjson: no document")

// Bind is used internally to Bind placeholders. It is available as a public API
// for when you are querying the database directly.
func Bind(stmt *sqlite.Stmt, i int, v any) error {
	switch v := v.(type) {
	default:
		return fmt.Errorf("sqjdb: unexpected value %v of type %T", v, v)
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

// SQL is part of a larger SQL query.
type SQL struct {
	Query string
	Args  []any
}

// ByID generates a where clause to select a document by ID.
func ByID(id string) SQL {
	return SQL{Query: "where data->>'ID' = ?", Args: []any{id}}
}

// Table provides access to a named table with the associated type.
// Use NewTable to create one.
type Table[T any] struct {
	Name    string
	qInsert string
}

// NewTable creates a new Table.
func NewTable[T any](name string) Table[T] {
	return Table[T]{
		Name:    name,
		qInsert: "insert into " + name + " (data) values (jsonb(?))",
	}
}

// Migrate runs the standard migrations, including creating the table if
// necessary. They are idempotent and should probably be run on application
// startup.
func (t *Table[T]) Migrate(conn *sqlite.Conn) error {
	qCreate := "create table if not exists " + t.Name + " (data blob)"
	if err := sqlitex.ExecuteTransient(conn, qCreate, nil); err != nil {
		return fmt.Errorf("sqjdb: creating table %q: %w", t.Name, err)
	}
	qIndexID := "create unique index if not exists " + t.Name +
		"_ID on " + t.Name + " (data->>'ID')"
	if err := sqlitex.ExecuteTransient(conn, qIndexID, nil); err != nil {
		return fmt.Errorf("sqjdb: creating ID index on %q: %w", t.Name, err)
	}
	return nil
}

// Insert a new document. If the document contains a non-empty ID, it will be
// returned as is. If the ID is empty, a shallow clone of the document will be
// returned with a generated ID set.
func (t *Table[T]) Insert(conn *sqlite.Conn, doc *T) (*T, error) {
	reflectV := reflect.Indirect(reflect.ValueOf(doc))
	vID := reflectV.FieldByName("ID")
	if !vID.IsValid() {
		return nil, fmt.Errorf("sqjdb: expected type %T to contain an ID field of type string", doc)
	}
	if vID.IsZero() {
		docCopy := *doc
		doc = &docCopy
		reflect.Indirect(reflect.ValueOf(doc)).FieldByName("ID").SetString(ulid.Make().String())
	}
	jsonS, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("sqjdb: failed to json.Marshal: %w", err)
	}
	stmt, err := conn.Prepare(t.qInsert)
	if err != nil {
		return nil, fmt.Errorf("sqjdb: failed to prepare %q: %w", t.qInsert, err)
	}
	stmt.BindText(1, string(jsonS))
	if _, err := stmt.Step(); err != nil {
		return nil, fmt.Errorf("sqjdb: inserting document in %q: %w", t.Name, err)
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
				return err
			}
			i++
		}
	}
	return nil
}

func (t *Table[T]) stepOne(stmt *sqlite.Stmt) (*T, error) {
	rowReturned, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !rowReturned {
		return nil, nil
	}
	jsonS := stmt.ColumnText(0)
	v := new(T)
	if err := json.Unmarshal([]byte(jsonS), v); err != nil {
		return nil, fmt.Errorf("sqjdb: invalid json from db: %w\n%s", err, jsonS)
	}
	return v, nil
}

// One returns a single document per the given query. It returns the error
// ErrNoDoc if no document is found.
func (t *Table[T]) One(conn *sqlite.Conn, sqls ...SQL) (*T, error) {
	var query strings.Builder
	query.WriteString("select json(data) from ")
	query.WriteString(t.Name)
	addSQLQuery(&query, sqls)
	query.WriteString(" limit 1")
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return nil, fmt.Errorf("sqjdb: failed to prepare: %q: %w", query.String(), err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return nil, err
	}
	v, err := t.stepOne(stmt)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNoDoc
	}
	return v, nil
}

// All returns all documents per the given query. It returns an empty slice with
// no error if no documents match.
func (t *Table[T]) All(conn *sqlite.Conn, sqls ...SQL) ([]*T, error) {
	var query strings.Builder
	query.WriteString("select json(data) from ")
	query.WriteString(t.Name)
	addSQLQuery(&query, sqls)
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return nil, fmt.Errorf("sqjdb: failed to prepare: %q: %w", query.String(), err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return nil, err
	}
	var docs []*T
	for {
		v, err := t.stepOne(stmt)
		if err != nil {
			return nil, err
		}
		if v == nil {
			break
		}
		docs = append(docs, v)
	}
	return docs, nil
}

// Delete one or more documents per the given query.
func (t *Table[T]) Delete(conn *sqlite.Conn, sqls ...SQL) error {
	var query strings.Builder
	query.WriteString("delete from ")
	query.WriteString(t.Name)
	addSQLQuery(&query, sqls)
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return fmt.Errorf("sqjdb: failed to prepare %q: %w", query.String(), err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("sqjdb: failed to delete: %w", err)
	}
	return nil
}

func (t *Table[T]) patchOrReplace(partQ string, conn *sqlite.Conn, doc *T, sqls []SQL) error {
	var query strings.Builder
	query.WriteString("update ")
	query.WriteString(t.Name)
	jsonS, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("sqjdb: failed to json.Marshal: %w", err)
	}
	sqls = slices.Concat([]SQL{{Query: partQ, Args: []any{jsonS}}}, sqls)
	addSQLQuery(&query, sqls)
	stmt, err := conn.Prepare(query.String())
	if err != nil {
		return fmt.Errorf("sqjdb: failed to prepare %q: %w", query.String(), err)
	}
	if err := bindSQLQuery(stmt, sqls); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("sqjdb: failed to execute %q: %w", query.String(), err)
	}
	return nil
}

// Patch applies the given update using jsonb_patch per the given query.
func (t *Table[T]) Patch(conn *sqlite.Conn, doc *T, sqls ...SQL) error {
	return t.patchOrReplace("set data = jsonb_patch(data, ?)", conn, doc, sqls)
}

// Replace replaces the document(s) per the given query.
func (t *Table[T]) Replace(conn *sqlite.Conn, doc *T, sqls ...SQL) error {
	return t.patchOrReplace("set data = jsonb(?)", conn, doc, sqls)
}
