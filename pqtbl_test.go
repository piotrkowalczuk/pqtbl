package pqtbl_test

import (
	"testing"

	"reflect"

	"github.com/piotrkowalczuk/pqtbl"
)

func TestTable_CreateQuery(t *testing.T) {
	success := []struct {
		expected string
		given    pqtbl.Table
	}{
		{
			expected: `CREATE TEMPORARY TABLE schema.user (
	username TEXT NOT NULL,
	password TEXT,
	created_at TIMESTAMPTZ
);`,
			given: pqtbl.Table{
				Name:      "user",
				Schema:    "schema",
				Collate:   "UTF-8",
				Temporary: true,
				Columns: []pqtbl.Column{
					{Name: "username", Type: pqtbl.DataTypeText, NotNull: true},
					{Name: "password", Type: pqtbl.DataTypeText},
					{Name: "created_at", Type: pqtbl.DataTypeTimestampTZ},
				},
			},
		},
		{
			expected: `CREATE TABLE IF NOT EXISTS table_name (
	id SERIAL,
	rel_id INTEGER,
	name TEXT,
	enabled BOOL,
	price DECIMAL,
	start_at TIMESTAMPTZ NOT NULL,
	end_at TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
	created_by INTEGER NOT NULL,
	updated_at TIMESTAMPTZ,
	updated_by INTEGER,
	slug TEXT NOT NULL,

	CONSTRAINT "public.table_name_name_key" UNIQUE (name),
	CONSTRAINT "public.table_name_pkey" PRIMARY KEY (id),
	CONSTRAINT "public.table_name_rel_id_fkey" FOREIGN KEY (rel_id) REFERENCES related_table (id),
	CONSTRAINT "public.table_name_slug_key" UNIQUE (slug),
	CONSTRAINT "public.table_name_start_at_end_at_check" CHECK ((start_at IS NULL AND end_at IS NULL) OR start_at < end_at)
);`,
			given: pqtbl.Table{
				Name:        "table_name",
				IfNotExists: true,
				Columns: []pqtbl.Column{
					{Name: "id", Type: pqtbl.DataTypeSerial, PrimaryKey: true},
					{
						Name:            "rel_id",
						Type:            pqtbl.DataTypeInteger,
						ReferenceColumn: "id",
						ReferenceTable:  "related_table",
					},
					{Name: "name", Type: pqtbl.DataTypeText, Unique: true},
					{Name: "enabled", Type: pqtbl.DataTypeBool},
					{Name: "price", Type: pqtbl.DataTypeDecimal},
					{Name: "start_at", Type: pqtbl.DataTypeTimestampTZ, NotNull: true},
					{Name: "end_at", Type: pqtbl.DataTypeTimestampTZ, NotNull: true},
					{Name: "created_at", Type: pqtbl.DataTypeTimestampTZ, NotNull: true, Default: pqtbl.FunctionNow},
					{Name: "created_by", Type: pqtbl.DataTypeInteger, NotNull: true},
					{Name: "updated_at", Type: pqtbl.DataTypeTimestampTZ},
					{Name: "updated_by", Type: pqtbl.DataTypeInteger},
					{Name: "slug", Type: pqtbl.DataTypeText, NotNull: true, Unique: true},
				},
				Constraints: []pqtbl.Constraint{
					pqtbl.Check("", "table_name", "(start_at IS NULL AND end_at IS NULL) OR start_at < end_at", "start_at", "end_at"),
				},
			},
		},
	}

	for _, data := range success {
		q, err := data.given.CreateQuery()

		if err != nil {
			t.Errorf("unexpected error: %s", err.Error())
			continue
		}

		if q != data.expected {
			t.Errorf("wrong query, expected:\n%s\nbut got:\n%s", data.expected, q)
		}
	}
}

func TestColumns_Exclude(t *testing.T) {
	success := []struct {
		given    pqtbl.Columns
		expected pqtbl.Columns
		exclude  []string
	}{
		{
			given:    pqtbl.Columns{"1", "2", "3"},
			expected: pqtbl.Columns{"3"},
			exclude:  []string{"1", "2"},
		},
		{
			given:    pqtbl.Columns{"id", "username", "password", "first_name", "last_name"},
			expected: pqtbl.Columns{"id", "username", "first_name", "last_name"},
			exclude:  []string{"password"},
		},
		{
			given:    pqtbl.Columns{"a", "b", "c"},
			expected: pqtbl.Columns{"a", "b", "c"},
			exclude:  []string{"d", "e"},
		},
	}

	for _, data := range success {
		same := reflect.DeepEqual(data.given, data.expected)
		got := data.given.Exclude(data.exclude...)

		if !reflect.DeepEqual(got, data.expected) {
			t.Errorf("wrong set of columns, expected %#v but got %#v", data.expected, got)
		}
		if reflect.DeepEqual(got, data.given) && !same {
			t.Errorf("source slice has been modified: %#v", data.given)
		}
	}
}

func TestColumns_Keep(t *testing.T) {
	success := []struct {
		given    pqtbl.Columns
		expected pqtbl.Columns
		keep     []string
	}{
		{
			given:    pqtbl.Columns{"1", "2", "3"},
			expected: pqtbl.Columns{"1", "2"},
			keep:     []string{"1", "2"},
		},
		{
			given:    pqtbl.Columns{"id", "username", "password", "first_name", "last_name"},
			expected: pqtbl.Columns{"password"},
			keep:     []string{"password"},
		},
		{
			given:    pqtbl.Columns{"a", "b", "c"},
			expected: pqtbl.Columns{},
			keep:     []string{"d", "e"},
		},
	}

	for _, data := range success {
		got := data.given.Keep(data.keep...)

		if !reflect.DeepEqual(got, data.expected) {
			t.Errorf("wrong set of columns, expected %#v but got %#v", data.expected, got)
		}
		if reflect.DeepEqual(got, data.given) {
			t.Errorf("source slice has been modified")
		}
	}
}

func TestColumns_WithPrefix(t *testing.T) {
	success := []struct {
		given    pqtbl.Columns
		expected pqtbl.Columns
		prefix   string
	}{
		{
			given:    pqtbl.Columns{"1", "2", "3"},
			expected: pqtbl.Columns{"longprefix.1", "longprefix.2", "longprefix.3"},
			prefix:   "longprefix",
		},
		{
			given:    pqtbl.Columns{"id", "username", "password", "first_name", "last_name"},
			expected: pqtbl.Columns{"a.id", "a.username", "a.password", "a.first_name", "a.last_name"},
			prefix:   "a",
		},
		{
			given:    pqtbl.Columns{"a", "b", "c"},
			expected: pqtbl.Columns{"1.a", "1.b", "1.c"},
			prefix:   "1",
		},
	}

	for _, data := range success {
		got := data.given.WithPrefix(data.prefix)

		if !reflect.DeepEqual(got, data.expected) {
			t.Errorf("wrong set of columns, expected %#v but got %#v", data.expected, got)
		}
		if reflect.DeepEqual(got, data.given) {
			t.Errorf("source slice has been modified")
		}
	}
}
