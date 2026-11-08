package libsqlhandler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Hrana JSON structures (docs: libsql/docs/HRANA_3_SPEC.md). Unknown fields
// are ignored, as the spec requires.

type hranaPipelineReq struct {
	Baton    *string           `json:"baton"`
	Requests *[]hranaStreamReq `json:"requests"`
}

type hranaCursorReq struct {
	Baton *string     `json:"baton"`
	Batch *hranaBatch `json:"batch"`
}

type hranaStreamReq struct {
	Type  string      `json:"type"`
	Stmt  *hranaStmt  `json:"stmt"`
	Batch *hranaBatch `json:"batch"`
	SQL   *string     `json:"sql"`
	SQLID *int32      `json:"sql_id"`
}

func (r *hranaStreamReq) UnmarshalJSON(b []byte) error {
	type plain hranaStreamReq
	if err := json.Unmarshal(b, (*plain)(r)); err != nil {
		return err
	}
	switch r.Type {
	case "execute":
		if r.Stmt == nil {
			return errors.New("missing field `stmt`")
		}
	case "batch":
		if r.Batch == nil {
			return errors.New("missing field `batch`")
		}
	case "store_sql":
		if r.SQL == nil || r.SQLID == nil {
			return errors.New("store_sql requires `sql` and `sql_id`")
		}
	case "close_sql":
		if r.SQLID == nil {
			return errors.New("missing field `sql_id`")
		}
	case "close", "sequence", "describe", "get_autocommit":
	default:
		return fmt.Errorf("unknown request type %q", r.Type)
	}
	return nil
}

type hranaStmt struct {
	SQL       *string         `json:"sql"`
	SQLID     *int32          `json:"sql_id"`
	Args      []hranaValue    `json:"args"`
	NamedArgs []hranaNamedArg `json:"named_args"`
	WantRows  *bool           `json:"want_rows"`
}

type hranaNamedArg struct {
	Name  string     `json:"name"`
	Value hranaValue `json:"value"`
}

// UnmarshalJSON requires both fields, like sqld: a missing value must not
// silently bind NULL.
func (a *hranaNamedArg) UnmarshalJSON(b []byte) error {
	var raw struct {
		Name  *string     `json:"name"`
		Value *hranaValue `json:"value"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if raw.Name == nil || raw.Value == nil {
		return errors.New("named argument requires `name` and `value`")
	}
	a.Name, a.Value = *raw.Name, *raw.Value
	return nil
}

type hranaBatch struct {
	Steps []hranaBatchStep `json:"steps"`
}

type hranaBatchStep struct {
	Condition *hranaCond `json:"condition"`
	Stmt      hranaStmt  `json:"stmt"`
}

type hranaCond struct {
	Type  string      `json:"type"`
	Step  *uint32     `json:"step"`
	Cond  *hranaCond  `json:"cond"`
	Conds []hranaCond `json:"conds"`
}

// validate checks that conditions only refer to steps before step i.
func (c *hranaCond) validate(i int) error {
	switch c.Type {
	case "ok", "error":
		if c.Step == nil || int64(*c.Step) >= int64(i) {
			return errProtocol("Invalid reference to step in a batch condition")
		}
	case "not":
		if c.Cond == nil {
			return errProtocol("missing field `cond`")
		}
		return c.Cond.validate(i)
	case "and", "or":
		if c.Conds == nil {
			return errProtocol("missing field `conds`")
		}
		for j := range c.Conds {
			if err := c.Conds[j].validate(i); err != nil {
				return err
			}
		}
	case "is_autocommit":
	default:
		return errProtocol(fmt.Sprintf("unknown batch condition %q", c.Type))
	}
	return nil
}

// hranaValue is a decoded SQLite value: nil, int64, float64, string or []byte.
type hranaValue struct{ v any }

func (v *hranaValue) UnmarshalJSON(b []byte) error {
	var raw struct {
		Type   string          `json:"type"`
		Value  json.RawMessage `json:"value"`
		Base64 *string         `json:"base64"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	isNull := len(raw.Value) == 0 || string(raw.Value) == "null"
	switch raw.Type {
	case "null":
		v.v = nil
	case "integer":
		var s string
		if isNull || json.Unmarshal(raw.Value, &s) != nil {
			return errors.New("integer value must be a decimal string")
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer value %q", s)
		}
		v.v = n
	case "float":
		var f float64
		if isNull || json.Unmarshal(raw.Value, &f) != nil {
			return errors.New("float value must be a number")
		}
		v.v = f
	case "text":
		var s string
		if isNull || json.Unmarshal(raw.Value, &s) != nil {
			return errors.New("text value must be a string")
		}
		v.v = s
	case "blob":
		if raw.Base64 == nil {
			return errors.New("blob value requires `base64`")
		}
		data, err := base64.StdEncoding.DecodeString(*raw.Base64)
		if err != nil {
			data, err = base64.RawStdEncoding.DecodeString(*raw.Base64)
		}
		if err != nil {
			return fmt.Errorf("invalid base64 in blob value: %w", err)
		}
		v.v = data
	default:
		return fmt.Errorf("unknown value type %q", raw.Type)
	}
	return nil
}

// outValue encodes a value read from SQLite.
type outValue struct{ v any }

func (v outValue) MarshalJSON() ([]byte, error) { return appendValue(nil, v.v), nil }

func appendValue(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, `{"type":"null"}`...)
	case int64:
		b = append(b, `{"type":"integer","value":"`...)
		b = strconv.AppendInt(b, x, 10)
		return append(b, `"}`...)
	case bool:
		if x {
			return append(b, `{"type":"integer","value":"1"}`...)
		}
		return append(b, `{"type":"integer","value":"0"}`...)
	case float64:
		b = append(b, `{"type":"float","value":`...)
		if math.IsInf(x, 0) || math.IsNaN(x) {
			b = append(b, "null"...) // as sqld does
		} else {
			b = appendFloat(b, x)
		}
		return append(b, '}')
	case []byte:
		// Unpadded, as sqld sends it.
		b = append(b, `{"type":"blob","base64":"`...)
		b = base64.RawStdEncoding.AppendEncode(b, x)
		return append(b, `"}`...)
	case time.Time:
		return appendText(b, formatTime(x))
	case string:
		return appendText(b, x)
	}
	return appendText(b, fmt.Sprint(v))
}

func appendText(b []byte, s string) []byte {
	b = append(b, `{"type":"text","value":`...)
	enc, _ := json.Marshal(s)
	b = append(b, enc...)
	return append(b, '}')
}

// appendFloat formats like encoding/json.
func appendFloat(b []byte, f float64) []byte {
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	start := len(b)
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		n := len(b) - start
		if n >= 4 && b[len(b)-4] == 'e' && b[len(b)-3] == '-' && b[len(b)-2] == '0' {
			b[len(b)-2] = b[len(b)-1]
			b = b[:len(b)-1]
		}
	}
	return b
}

type hranaCol struct {
	Name     string  `json:"name"`
	Decltype *string `json:"decltype"`
}

type hranaStmtResult struct {
	Cols             []hranaCol   `json:"cols"`
	Rows             [][]outValue `json:"rows"`
	AffectedRowCount int64        `json:"affected_row_count"`
	LastInsertRowid  *string      `json:"last_insert_rowid"`
	RowsRead         int64        `json:"rows_read"`
	RowsWritten      int64        `json:"rows_written"`
	QueryDurationMs  float64      `json:"query_duration_ms"`
}

type hranaBatchResult struct {
	StepResults []*hranaStmtResult `json:"step_results"`
	StepErrors  []*hranaError      `json:"step_errors"`
}

type hranaDescribeResult struct {
	Params     []hranaDescribeParam `json:"params"`
	Cols       []hranaCol           `json:"cols"`
	IsExplain  bool                 `json:"is_explain"`
	IsReadonly bool                 `json:"is_readonly"`
}

type hranaDescribeParam struct {
	Name *string `json:"name"`
}

type hranaStreamResp struct {
	Type         string `json:"type"`
	Result       any    `json:"result,omitempty"`
	IsAutocommit *bool  `json:"is_autocommit,omitempty"`
}

type hranaStreamResult struct {
	Type     string           `json:"type"`
	Response *hranaStreamResp `json:"response,omitempty"`
	Error    *hranaError      `json:"error,omitempty"`
}

type hranaPipelineResp struct {
	Baton   *string             `json:"baton"`
	BaseURL *string             `json:"base_url"`
	Results []hranaStreamResult `json:"results"`
}

type hranaCursorResp struct {
	Baton   *string `json:"baton"`
	BaseURL *string `json:"base_url"`
}

// hranaError is an in-band error of one statement or request; the stream
// stays usable.
type hranaError struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

func (e *hranaError) Error() string { return e.Message }

// protocolError fails the whole HTTP request with 400 and closes the stream.
type protocolError struct{ msg string }

func (e *protocolError) Error() string { return e.msg }

func errProtocol(msg string) error { return &protocolError{msg} }

// sqliteCodes names primary result codes the way sqld does.
var sqliteCodes = map[int]string{
	2: "SQLITE_INTERNAL", 3: "SQLITE_PERM", 4: "SQLITE_ABORT", 5: "SQLITE_BUSY", 6: "SQLITE_LOCKED",
	7: "SQLITE_NOMEM", 8: "SQLITE_READONLY", 9: "SQLITE_INTERRUPT", 10: "SQLITE_IOERR", 11: "SQLITE_CORRUPT",
	12: "SQLITE_NOTFOUND", 13: "SQLITE_FULL", 14: "SQLITE_CANTOPEN", 15: "SQLITE_PROTOCOL", 17: "SQLITE_SCHEMA",
	18: "SQLITE_TOOBIG", 19: "SQLITE_CONSTRAINT", 20: "SQLITE_MISMATCH", 21: "SQLITE_MISUSE", 22: "SQLITE_NOLFS",
	23: "SQLITE_AUTH", 25: "SQLITE_RANGE", 26: "SQLITE_NOTADB",
}

// sqliteError converts a driver error. The code is known for drivers whose
// errors have a Code() int method (modernc.org/sqlite); otherwise, and for
// SQLITE_ERROR itself, it is SQLITE_UNKNOWN, like sqld.
func sqliteError(err error) *hranaError {
	code := "SQLITE_UNKNOWN"
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		if name, ok := sqliteCodes[coded.Code()&0xff]; ok {
			code = name
		}
	}
	return &hranaError{Message: err.Error(), Code: code}
}
