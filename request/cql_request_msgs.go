package request

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/parsers/cql_parser"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/google/uuid"
)

const HTTPHeaderAPICQL = "cql"

// ok row to return for successful writes
var okRow = map[uint8]cql_data_model.LocusDBValue{0: cql_data_model.NewTextValue("ok")}
var okRowColIds = []uint8{0}
var okRowNames = []string{"result"}
var okRowColTypes = []cql_data_model.LocusDBType{cql_data_model.Text}

func init() {
	gob.Register(CqlRequest{})
	gob.Register(CqlReply{})
}

/***********************************************
 * Client-Replica Messages For CQL Mode        *
 **********************************************/

// KVRequest is bench request with http response channel
type CqlRequest struct {
	CqlQueries string `json:"command"` // this is a semicolon-separated list of raw queries that will be parsed at the node_assembler.

	// alternatively, we can parse at the client_driver, and provide structs for inserts, updates, selects, deletes, etc
	ParseAtSrvr   bool
	CqlInserts    []cql_parser.InsertStatement
	CqlUpdates    []cql_parser.UpdateStatement
	CqlDeletes    []cql_parser.DeleteStatement
	CqlSelects    []cql_parser.SelectQuery
	CqlCreateTbls []cql_parser.CreateTableStatement

	ClientID  ids.ID        `json:"client_id"`
	RequestId ids.CommandID `json:"client_request_id"` // client-specific command for identifying response
	Timestamp int64         `json:"timestamp"`

	C chan CqlReply `json:"-"`
}

// KVReply replies to the current request
func (r *CqlRequest) Reply(reply CqlReply) {
	r.C <- reply
}

func (r CqlRequest) String() string {
	return fmt.Sprintf("CqlRequest {ClientId=%v ClientCmdId=%d, queries=%v}", r.ClientID, r.RequestId, r.CqlQueries)
}

type CqlOutput struct {
	Rows         []cql_data_model.Row
	TableName    string
	TableUUID    uuid.UUID
	DisplayOrder []uint8                      // order in which columns appear in the query and output
	ColumnNames  []string                     // names/aliases of columns in the same order
	ColumnTypes  []cql_data_model.LocusDBType // data types of said columns
	LeaderID     ids.ID                       // optional leader id of the node responsible for producing the result
}

func (c *CqlOutput) UnmarshalJSON(b []byte) error {
	type tempCqlOut struct {
		Rows         []map[int]map[string]interface{}
		TableName    string
		TableUUID    uuid.UUID
		DisplayOrder []uint8                      // order in which columns appear in the query and output
		ColumnNames  []string                     // names/aliases of columns in the same order
		ColumnTypes  []cql_data_model.LocusDBType // data types of said columns
		LeaderId     ids.ID
	}

	var temp tempCqlOut
	err := json.Unmarshal(b, &temp)
	if err != nil {
		return err
	}

	var realRows = make([]cql_data_model.Row, len(temp.Rows))
	// now create proper rows with LocusDBValue cells
	for i, row := range temp.Rows {
		realRow := make(map[uint8]cql_data_model.LocusDBValue, len(row))
		for displayId, colId := range temp.DisplayOrder {
			var tempval cql_data_model.LocusDBValue
			switch row[int(colId)]["Val"].(type) {
			case string:
				tempval = cql_data_model.NewTextValue(row[int(colId)]["Val"].(string))
			case int:
				tempval = cql_data_model.NewInt(row[int(colId)]["Val"].(int32))
			case int64:
				tempval = cql_data_model.NewBigInt(row[int(colId)]["Val"].(int64))
			case float32:
				tempval = cql_data_model.NewFloat(row[int(colId)]["Val"].(float32))
			case float64:
				tempval = cql_data_model.NewDoubleValue(row[int(colId)]["Val"].(float64))
			case bool:
				tempval = cql_data_model.NewBooleanValue(row[int(colId)]["Val"].(bool))
			default:
				tempval = cql_data_model.NewTextValue(fmt.Sprintf("%v", row[int(colId)]["Val"]))
			}
			val, err := tempval.CastIfPossible(temp.ColumnTypes[displayId])
			if err != nil {
				return err
			}
			realRow[colId] = val
		}
		realRows[i] = realRow
	}

	c.Rows = realRows
	c.DisplayOrder = temp.DisplayOrder
	c.ColumnTypes = temp.ColumnTypes
	c.ColumnNames = temp.ColumnNames
	c.TableName = temp.TableName
	c.TableUUID = temp.TableUUID
	c.LeaderID = temp.LeaderId

	return nil
}

func NewOkRowCqlOut(tableUUID uuid.UUID, tableName string, leaderid ids.ID) CqlOutput {

	cqlOut := CqlOutput{
		TableUUID:    tableUUID,
		TableName:    tableName,
		Rows:         []cql_data_model.Row{okRow},
		DisplayOrder: okRowColIds,
		ColumnNames:  okRowNames,
		ColumnTypes:  okRowColTypes,
		LeaderID:     leaderid,
	}

	return cqlOut
}

func (cqlout *CqlOutput) String() string {
	var sb strings.Builder
	_, err := fmt.Fprintf(&sb, "%s\n", cqlout.TableName)
	if err != nil {
		log.Errorf("Error formatting output: %v", err)
	}

	var sbrows strings.Builder

	// compute cell length for formatting
	var cellLen = make([]int, len(cqlout.DisplayOrder))
	for i, colname := range cqlout.ColumnNames {
		cellLen[i] = len(colname)
	}

	for _, r := range cqlout.Rows {
		for i, displayId := range cqlout.DisplayOrder {
			if col, exists := r[displayId]; exists {
				colStr := col.String()
				if len(colStr) > cellLen[i] {
					cellLen[i] = len(colStr)
				}
			}
		}
	}

	for _, r := range cqlout.Rows {
		for i, displayId := range cqlout.DisplayOrder {
			if col, exists := r[displayId]; exists {
				cellFmt := " %" + strconv.Itoa(cellLen[i]) + "s |"
				_, err = fmt.Fprintf(&sbrows, cellFmt, col.String())
				if err != nil {
					log.Errorf("Error formatting output: %v", err)
				}
			} else {
				log.Errorf("Could not find column id %v in the output", displayId)
			}
		}
		_, err = fmt.Fprintf(&sbrows, "\n")
		if err != nil {
			log.Errorf("Error formatting output: %v", err)
		}
	}

	rowLen := 0
	for i, colName := range cqlout.ColumnNames {
		rowLen += cellLen[i] + 3
		cellFmt := " %" + strconv.Itoa(cellLen[i]) + "s |"
		_, err = fmt.Fprintf(&sb, cellFmt, colName)
		if err != nil {
			log.Errorf("Error formatting output: %v", err)
		}
	}

	veticalDivider := strings.Repeat("-", rowLen)
	strfmt := "\n%" + strconv.Itoa(rowLen) + "s"
	_, err = fmt.Fprintf(&sb, strfmt, veticalDivider)
	if err != nil {
		log.Errorf("Error formatting output: %v", err)
	}

	_, err = fmt.Fprintf(&sb, "\n%s", sbrows.String())
	if err != nil {
		log.Errorf("Error formatting output: %v", err)
	}
	sb.WriteString("\n")
	return sb.String()
}

// CqlReply includes all info that might reply to back the client_driver for the corresponding request
type CqlReply struct {
	ClientID  ids.ID
	RequestId ids.CommandID `json:"client_command_id"` // client-specific command for identifying response
	CqlOuts   []CqlOutput
	Timestamp int64
	Errs      []string
}

func (r CqlReply) String() string {
	return fmt.Sprintf("CqlReply {ts=%d, clientId=%v}", r.Timestamp, r.ClientID)
}
