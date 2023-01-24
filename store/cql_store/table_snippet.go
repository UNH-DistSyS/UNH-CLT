package cql_store

import "github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"

type TableSnippet struct {
	TableSpec *LocusDBTableSpec
	Rows      []cql_data_model.Row
}

func NewTableSnippet(tableSpec *LocusDBTableSpec) *TableSnippet {
	return &TableSnippet{
		TableSpec: tableSpec,
		Rows:      make([]cql_data_model.Row, 0),
	}
}

func (fr *TableSnippet) Append(row cql_data_model.Row) {
	fr.Rows = append(fr.Rows, row)
}

func (fr *TableSnippet) Size() int {
	return len(fr.Rows)
}
