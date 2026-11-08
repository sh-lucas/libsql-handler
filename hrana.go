package libsqlhandler

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Hrana over HTTP, JSON encoding, versions 2 and 3: the protocol of the
// official libSQL clients. A stream is a session addressed by a baton that
// changes with every response, so a stale or replayed baton is rejected.

var errInvalidBaton = errors.New("Received an invalid baton")

func writeHranaError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, hranaError{Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		body, _ = json.Marshal(hranaError{Message: "encoding response: " + err.Error()})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// readHrana reads and decodes a request body. It runs before h.enter, so
// Close never waits on a slow client. On a decode error it also returns the
// baton if one is readable, so the stream can be closed: the client must
// treat it as invalid after a 4xx.
func readHrana(r *http.Request, v any) (*string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errProtocol("reading request body: " + err.Error())
	}
	if err := json.Unmarshal(body, v); err != nil {
		var partial struct {
			Baton *string `json:"baton"`
		}
		_ = json.Unmarshal(body, &partial)
		return partial.Baton, errProtocol("Cannot deserialize client message from JSON: " + err.Error())
	}
	return nil, nil
}

// rejectRequest answers 400 and closes the stream the request named, if any.
func (h *LibSQLHandler) rejectRequest(ctx context.Context, w http.ResponseWriter, baton *string, err error) {
	if baton != nil {
		if s, _ := h.openStream(ctx, baton); s != nil {
			h.endStream(s, true)
		}
	}
	writeHranaError(w, http.StatusBadRequest, err.Error())
}

// openStream returns the stream named by baton, locked, or a new one.
func (h *LibSQLHandler) openStream(ctx context.Context, baton *string) (*session, error) {
	if baton == nil {
		s, err := h.create(ctx)
		if err == nil {
			// Streams end by close or expiry only; never evicted as one-shot.
			s.reused.Store(true)
		}
		return s, err
	}
	s, err := h.lookup(*baton)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errInvalidBaton
	}
	s.mu.Lock()
	// A racing request with the same baton got here first and rotated it.
	if s.conn == nil || s.key != *baton {
		s.mu.Unlock()
		return nil, errInvalidBaton
	}
	return s, nil
}

func streamErrorStatus(err error) int {
	var pe *protocolError
	if errors.Is(err, errInvalidBaton) || errors.Is(err, errSessionLost) || errors.As(err, &pe) {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

// endStream releases a stream after a request and returns its next baton,
// or nil once closed.
func (h *LibSQLHandler) endStream(s *session, closed bool) *string {
	if closed {
		h.closeSession(s)
		s.mu.Unlock()
		return nil
	}
	baton := h.rekey(s)
	h.release(s)
	return baton
}

func (h *LibSQLHandler) serveHranaPipeline(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(w, r, writeHranaError) {
		return
	}
	var req hranaPipelineReq
	badBaton, decodeErr := readHrana(r, &req)
	ctx, done, ok := h.enter(w, r, writeHranaError)
	if !ok {
		return
	}
	defer done()
	if decodeErr != nil {
		h.rejectRequest(ctx, w, badBaton, decodeErr)
		return
	}
	if req.Requests == nil {
		h.rejectRequest(ctx, w, req.Baton, errProtocol("missing field `requests`"))
		return
	}
	s, err := h.openStream(ctx, req.Baton)
	if err != nil {
		writeHranaError(w, streamErrorStatus(err), err.Error())
		return
	}

	results := make([]hranaStreamResult, 0, len(*req.Requests))
	closed := false
	for i := range *req.Requests {
		sr := &(*req.Requests)[i]
		if closed {
			err = errProtocol("Stream for this baton was closed")
			break
		}
		var resp *hranaStreamResp
		var herr *hranaError
		resp, herr, err = s.runRequest(ctx, sr)
		if err != nil {
			break
		}
		if herr != nil {
			results = append(results, hranaStreamResult{Type: "error", Error: herr})
			continue
		}
		results = append(results, hranaStreamResult{Type: "ok", Response: resp})
		closed = sr.Type == "close"
	}
	if err != nil {
		// The stream state is unknown to the client now: drop it, rolling back.
		h.endStream(s, true)
		writeHranaError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ctx.Err() != nil {
		// The client went away (or the handler is closing) and will never see
		// the next baton: don't keep its transaction holding the write lock.
		h.endStream(s, true)
		writeHranaError(w, http.StatusServiceUnavailable, "request canceled")
		return
	}
	writeJSON(w, http.StatusOK, hranaPipelineResp{Baton: h.endStream(s, closed), Results: results})
}

// runRequest executes one stream request.
func (s *session) runRequest(ctx context.Context, sr *hranaStreamReq) (*hranaStreamResp, *hranaError, error) {
	resp := &hranaStreamResp{Type: sr.Type}
	switch sr.Type {
	case "close":
	case "close_sql":
		delete(s.sqls, *sr.SQLID)
	case "execute":
		res, herr, err := s.execCollect(ctx, sr.Stmt)
		if herr != nil || err != nil {
			return nil, herr, err
		}
		resp.Result = res
	case "batch":
		n := len(sr.Batch.Steps)
		br := &hranaBatchResult{StepResults: make([]*hranaStmtResult, n), StepErrors: make([]*hranaError, n)}
		err := s.runBatch(sr.Batch, func(i int, st *hranaStmt) (bool, error) {
			res, herr, err := s.execCollect(ctx, st)
			br.StepResults[i], br.StepErrors[i] = res, herr
			return herr == nil, err
		})
		if err != nil {
			return nil, nil, err
		}
		resp.Result = br
	case "sequence":
		text, err := s.resolveSQL(sr.SQL, sr.SQLID)
		if err != nil {
			return nil, nil, err
		}
		s.log.Debug("executing", "sql", text)
		if _, err := s.conn.ExecContext(ctx, text); err != nil {
			return nil, sqliteError(err), nil
		}
	case "describe":
		text, err := s.resolveSQL(sr.SQL, sr.SQLID)
		if err != nil {
			return nil, nil, err
		}
		res, herr := s.describe(ctx, text)
		if herr != nil {
			return nil, herr, nil
		}
		resp.Result = res
	case "store_sql":
		if _, dup := s.sqls[*sr.SQLID]; dup {
			return nil, nil, errProtocol(fmt.Sprintf("SQL text %d already exists", *sr.SQLID))
		}
		if s.sqls == nil {
			s.sqls = make(map[int32]string)
		}
		s.sqls[*sr.SQLID] = *sr.SQL
	case "get_autocommit":
		autocommit := !s.inTx()
		resp.IsAutocommit = &autocommit
	}
	return resp, nil, nil
}

type cursorStepBegin struct {
	Type string     `json:"type"`
	Step int        `json:"step"`
	Cols []hranaCol `json:"cols"`
}

// cursorStepEnd carries last_insert_rowid as a JSON number, unlike
// StmtResult: that is what sqld sends and the Rust client parses.
type cursorStepEnd struct {
	Type             string `json:"type"`
	AffectedRowCount int64  `json:"affected_row_count"`
	LastInsertRowid  *int64 `json:"last_insert_rowid"`
}

type cursorStepError struct {
	Type  string      `json:"type"`
	Step  int         `json:"step"`
	Error *hranaError `json:"error"`
}

type cursorRow struct {
	Type string     `json:"type"`
	Row  []outValue `json:"row"`
}

type cursorError struct {
	Type  string      `json:"type"`
	Error *hranaError `json:"error"`
}

// serveHranaCursor runs a batch and streams its results as JSON lines, so
// a large result never sits in memory.
func (h *LibSQLHandler) serveHranaCursor(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(w, r, writeHranaError) {
		return
	}
	var req hranaCursorReq
	badBaton, decodeErr := readHrana(r, &req)
	ctx, done, ok := h.enter(w, r, writeHranaError)
	if !ok {
		return
	}
	defer done()
	if decodeErr != nil {
		h.rejectRequest(ctx, w, badBaton, decodeErr)
		return
	}
	if req.Batch == nil {
		h.rejectRequest(ctx, w, req.Baton, errProtocol("missing field `batch`"))
		return
	}
	s, err := h.openStream(ctx, req.Baton)
	if err != nil {
		writeHranaError(w, streamErrorStatus(err), err.Error())
		return
	}
	if err := s.validateBatch(req.Batch); err != nil {
		h.endStream(s, true)
		writeHranaError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The next baton goes out first; the stream stays locked until the end.
	baton := h.rekey(s)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	rc := http.NewResponseController(w)
	// flush sends what is buffered: the header and each finished step go out
	// at once, so a client never waits on a slow step to see earlier results.
	flush := func() error {
		if err := bw.Flush(); err != nil {
			return err
		}
		if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		return nil
	}
	if err := enc.Encode(hranaCursorResp{Baton: baton}); err != nil || flush() != nil {
		h.endStream(s, true)
		return
	}
	err = s.runBatch(req.Batch, func(i int, st *hranaStmt) (bool, error) {
		sum, herr, err := s.execStmt(ctx, st,
			func(cols []hranaCol) error { return enc.Encode(cursorStepBegin{"step_begin", i, cols}) },
			func(vals []any) error {
				row := make([]outValue, len(vals))
				for j, v := range vals {
					row[j] = outValue{v}
				}
				return enc.Encode(cursorRow{"row", row})
			})
		if err != nil {
			return false, err
		}
		entry := any(cursorStepEnd{"step_end", sum.affectedRows, sum.lastRowid})
		if herr != nil {
			entry = cursorStepError{"step_error", i, herr}
		}
		if err := enc.Encode(entry); err != nil {
			return false, err
		}
		return herr == nil, flush()
	})
	if err == nil && ctx.Err() != nil {
		err = errors.New("request canceled")
	}
	if err != nil {
		// The client went away mid-stream: close the stream, like the pipeline does.
		_ = enc.Encode(cursorError{"error", &hranaError{Message: err.Error()}})
		_ = flush()
		h.endStream(s, true)
		return
	}
	_ = flush()
	h.release(s)
}
