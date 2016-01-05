package pqtbl

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/piotrkowalczuk/pqcnstr"
)

const (
	DataTypeSerial       = "SERIAL"
	DataTypeBigSerial    = "BIGSERIAL"
	DataTypeBool         = "BOOL"
	DataTypeDecimal      = "DECIMAL"
	DataTypeInteger      = "INTEGER"
	DataTypeSmallInteger = "SMALLINT"
	DataTypeBigInteger   = "BIGINT"
	DataTypeText         = "TEXT"
	DataTypeVarchar      = "VARCHAR"
	DataTypeTimestamp    = "TIMESTAMP"
	DataTypeTimestampTZ  = "TIMESTAMPTZ"
	DataTypeMoney        = "MONEY"
	FunctionNow          = "NOW()"
)

var (
	ErrMissingTableName    = errors.New("pqtbl: missing table name")
	ErrMissingTableColumns = errors.New("pqtbl: missing table columns")
)

type Table struct {
	Name, Schema, Collate, TableSpace string
	IfNotExists, Temporary            bool
	Columns                           []Column
	Constraints                       []Constraint
}

func (t *Table) CreateQuery() (string, error) {
	if t.Name == "" {
		return "", ErrMissingTableName
	}
	if len(t.Columns) == 0 {
		return "", ErrMissingTableColumns
	}
	constraints, err := t.constraints()
	if err != nil {
		return "", err
	}

	buf := bytes.NewBufferString("CREATE ")
	if t.Temporary {
		buf.WriteString("TEMPORARY ")
	}
	buf.WriteString("TABLE ")
	if t.IfNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}
	if t.Schema != "" {
		buf.WriteString(t.Schema)
		buf.WriteRune('.')
		buf.WriteString(t.Name)
	} else {
		buf.WriteString(t.Name)
	}
	buf.WriteString(" (\n")
	for i, c := range t.Columns {
		buf.WriteRune('	')
		buf.WriteString(c.Name)
		buf.WriteRune(' ')
		buf.WriteString(c.Type)
		if c.Collate != "" {
			buf.WriteRune(' ')
			buf.WriteString(c.Collate)
		}
		if c.Default != "" {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(c.Default)
		}
		if c.NotNull {
			buf.WriteString(" NOT NULL")
		}
		if i < len(t.Columns)-1 || len(constraints) > 0 {
			buf.WriteRune(',')
		}
		buf.WriteRune('\n')
	}

	if len(constraints) > 0 {
		buf.WriteRune('\n')
	}

	for i, query := range constraints {
		buf.WriteString("	")
		buf.WriteString(query)
		if i < len(constraints)-1 {
			buf.WriteRune(',')
		}
		buf.WriteRune('\n')
	}

	buf.WriteString(");")

	return buf.String(), nil
}

func (t *Table) constraints() ([]string, error) {
	constraints := make([]string, 0, len(t.Columns)+len(t.Constraints))

	for _, c := range t.Columns {
		if c.Unique && !c.PrimaryKey {
			constraints = append(constraints, uniqueConstraintQuery(t.Schema, t.Name, c.Name))
		}
		if c.PrimaryKey && !c.Unique {
			constraints = append(constraints, primaryKeyConstraintQuery(t.Schema, t.Name, c.Name))
		}
		if c.isReference() {
			if !c.isValidReference() {
				return nil, fmt.Errorf("pqtbl: invalid foreign key column schema: '%s', table: '%s', column: '%s'", c.ReferenceSchema, c.ReferenceTable, c.ReferenceColumn)
			}

			constraints = append(constraints, foreignKeyConstraintQuery(t.Schema, t.Name, []string{c.Name}, c.ReferenceSchema, c.ReferenceTable, []string{c.ReferenceColumn}))
		}

		if c.Check != "" {
			constraints = append(constraints, checkConstraintQuery(t.Schema, t.Name, c.Check, c.Name))
		}
	}

	for _, c := range t.Constraints {
		if c.Unique && !c.PrimaryKey {
			constraints = append(constraints, uniqueConstraintQuery(t.Schema, t.Name, c.Columns...))
		}
		if c.PrimaryKey && !c.Unique {
			constraints = append(constraints, primaryKeyConstraintQuery(t.Schema, t.Name, c.Columns...))
		}
		if c.isReference() {
			if !c.isValidReference() {
				return nil, fmt.Errorf("pqtbl: invalid foreign key column schema: '%s', table: '%s', columns: '%#v'", c.ReferenceSchema, c.ReferenceTable, c.ReferenceColumns)
			}

			constraints = append(constraints, foreignKeyConstraintQuery(t.Schema, t.Name, c.Columns, c.ReferenceSchema, c.ReferenceTable, c.ReferenceColumns))
		}

		if c.Check != "" {
			constraints = append(constraints, checkConstraintQuery(t.Schema, t.Name, c.Check, c.Columns...))
		}
	}

	sort.Strings(constraints)
	return constraints, nil
}

type Column struct {
	Name, Type, Collate, Default, Check              string
	NotNull, Unique, PrimaryKey                      bool
	ReferenceTable, ReferenceColumn, ReferenceSchema string
}

func (c *Column) isReference() bool {
	return c.ReferenceColumn != "" || c.ReferenceTable != "" || c.ReferenceSchema != ""
}

func (c *Column) isValidReference() bool {
	return c.ReferenceColumn != "" && c.ReferenceTable != ""
}

// Constraints ...
func (c *Column) Constraints(schema, table string) []pqcnstr.Constraint {
	cnstrs := []pqcnstr.Constraint{}
	if c.Unique {
		cnstrs = append(cnstrs, pqcnstr.Unique(schema, table, c.Name))
	}
	if c.PrimaryKey {
		cnstrs = append(cnstrs, pqcnstr.PrimaryKey(schema, table))
	}
	if c.isReference() {
		cnstrs = append(cnstrs, pqcnstr.ForeignKey(schema, table, c.Name))
	}
	if c.Check != "" {
		cnstrs = append(cnstrs, pqcnstr.Check(schema, table, c.Name))
	}

	return cnstrs
}

// GoType returns go specific type if possible, otherhise empty string and false.
func (c Column) GoType() (string, bool) {
	optional := c.NotNull || c.PrimaryKey

	switch c.Type {
	case DataTypeText:
		if optional {
			return "string", true
		}
		return "nilt.String", true
	case DataTypeBool:
		if optional {
			return "bool", true
		}
		return "nilt.Bool", true
	case DataTypeSmallInteger:
		return "int16", true
	case DataTypeInteger:
		return "int32", true
	case DataTypeBigInteger:
		if optional {
			return "int64", true
		}
		return "nilt.Int64", true
	case DataTypeSerial:
		if optional {
			return "uint32", true
		}
	case DataTypeBigSerial:
		if optional {
			return "uint64", true
		}
	case DataTypeTimestamp, DataTypeTimestampTZ:
		if optional {
			return "time.Time", true
		}
		return "*time.Time", true
	}

	if strings.HasPrefix(c.Type, DataTypeVarchar) {
		return "string", true
	}

	return "", false
}

// Columns ...
type Columns []string

// Keep creates copy without provided columns.
func (c Columns) Exclude(names ...string) Columns {
	l := len(c) - len(names)
	if l <= 0 {
		return nil
	}
	columns := make(Columns, 0, l)

ColumnsLoop:
	for _, column := range c {
		for _, name := range names {
			if column == name {
				continue ColumnsLoop
			}
		}

		columns = append(columns, column)
	}

	return columns
}

// Keep creates copy that contains only provided columns.
func (c Columns) Keep(names ...string) Columns {
	l := len(c) - len(names)
	if l <= 0 {
		return nil
	}
	columns := make([]string, 0, l)

	for _, column := range c {
		for _, name := range names {
			if column == name {
				columns = append(columns, column)
			}
		}
	}

	return columns
}

// WithPrefix creates copy, that each column name is prefixed with given string.
func (c Columns) WithPrefix(prefix string) Columns {
	columns := make(Columns, 0, len(c))

	for _, column := range c {
		col := column
		col = prefix + "." + col

		columns = append(columns, col)
	}

	return columns
}

// GoString implements fmt.GoStringer interface.
func (c Columns) GoString() string {
	buf := bytes.NewBufferString("[")

	for i, column := range c {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(column)
	}
	buf.WriteString("]")

	return buf.String()
}

// Join ...
func (c Columns) Join(sep string) string {
	tmp := make([]string, len(c))
	for _, cc := range c {
		tmp = append(tmp, cc)
	}

	return strings.Join(tmp, sep)
}

// Constraint ...
type Constraint struct {
	Name, Check, Default, OnDelete, OnUpdate string
	NotNull, Null, Unique, PrimaryKey        bool
	Columns                                  []string
	ReferenceSchema, ReferenceTable          string
	ReferenceColumns                         []string
	// TODO: exclude not implemented
}

// Check ...
func Check(schema, table, check string, columns ...string) Constraint {
	return Constraint{
		Name:    pqcnstr.Check(schema, table, columns...).String(),
		Check:   check,
		Columns: columns,
	}
}

func (c *Constraint) isReference() bool {
	return len(c.ReferenceColumns) != 0 || c.ReferenceTable != "" || c.ReferenceSchema != ""
}

func (c *Constraint) isValidReference() bool {
	return len(c.ReferenceColumns) != 0 && c.ReferenceTable != ""
}

func uniqueConstraintQuery(schema, table string, columns ...string) string {
	return fmt.Sprintf(`CONSTRAINT "%s" UNIQUE (%s)`, pqcnstr.Unique(schema, table, columns...).String(), strings.Join(columns, ", "))
}

func primaryKeyConstraintQuery(schema, table string, columns ...string) string {
	return fmt.Sprintf(`CONSTRAINT "%s" PRIMARY KEY (%s)`, pqcnstr.PrimaryKey(schema, table).String(), strings.Join(columns, ", "))
}

func foreignKeyConstraintQuery(schema, table string, columns []string, referenceSchema, referenceTable string, referenceColumns []string) string {
	var reference string
	if referenceSchema == "" {
		reference = referenceTable
	} else {
		reference = referenceSchema + "." + referenceTable
	}

	return fmt.Sprintf(`CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES %s (%s)`, pqcnstr.ForeignKey(schema, table, columns...).String(), strings.Join(columns, ", "), reference, strings.Join(referenceColumns, ", "))
}

func checkConstraintQuery(schema, table, check string, columns ...string) string {
	return fmt.Sprintf(`CONSTRAINT "%s" CHECK (%s)`, pqcnstr.Check(schema, table, columns...).String(), check)
}
