package cql_data_model

type Row map[uint8]LocusDBValue

func (r Row) SetCell(colId uint8, value LocusDBValue) {
	r[colId] = value
}
