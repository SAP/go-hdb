//go:build go1.27

package driver

import (
	"context"
	"errors"
	"io"
	"time"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// readResultPart reads a protocol result part. On Go 1.27+ lobs are read via
// session.readLobComplete, so the lob reader is not needed anymore. The unused
// parameter is only kept to share the call signature with the go1.26 build -
// drop it when the go1.26 code is removed after the 1.28 release.
func readResultPart(ctx context.Context, pi *p.PartInfo, part p.ResultPartDecoder, _ any) error {
	return pi.ReadResultPart(ctx, part)
}

// readLobComplete reads a complete lob value into wr. Value is the lob field value to scan.
func (s *session) readLobComplete(descr *p.LobOutDescr, wr io.Writer) error {
	request := &p.ReadLobRequest{ID: descr.LocatorID(), ChunkSize: s.attrs.lobChunkSize}
	reply := descr.NewReadLobReply()

	data, numChar, err := descr.Bytes(s.lobReadDecoder)
	opt := descr.Opt
	for err == nil {
		_, err = wr.Write(data)
		if err != nil {
			break
		}
		if opt.IsLastData() {
			break
		}
		request.Ofs += int64(numChar)
		if err = s.readLob(request, reply); err != nil {
			break
		}
		data, numChar, err = reply.Bytes(s.lobReadDecoder)
		opt = reply.Opt
	}
	// A Lob (possibly embedded) hides its pipe behind Write.
	if lwr, ok := wr.(interface{ lobWriter() io.Writer }); ok {
		wr = lwr.lobWriter()
	}
	// if the writer is a pipe-end -> close at the end
	if pwr, ok := wr.(*io.PipeWriter); ok {
		if err != nil {
			pwr.CloseWithError(err)
		} else {
			pwr.Close()
		}
	}
	return err
}

// readLob reads a single lob data package. The complete lob is read by
// session.readLobComplete which calls this method per data package.
func (s *session) readLob(request *p.ReadLobRequest, reply *p.ReadLobReply) error {
	defer s.metrics.addSQLTimeValue(sqlTimeFetchLob, time.Now())

	ctx := context.Background()

	if err := s.pwr.Write(ctx, p.MtWriteLob, false, request); err != nil {
		return err
	}

	for pi, err := range s.prd.Parts(ctx) {
		if err != nil {
			return err
		}
		switch pi.Header.Kind() {
		case p.PkError:
			if err := pi.ReadHDBErrors(ctx); err != nil {
				// mirrors the 1.26 scanLob wrapping of the HDB "invalid lob locator id"
				// error so callers see the friendlier errInvalidLobLocatorID.
				if dbErr, ok := errors.AsType[Error](err); ok && dbErr.Code() == p.HdbErrWhileParsingProtocol {
					return errInvalidLobLocatorID
				}
				return err
			}
		case p.PkReadLobReply:
			if err := pi.ReadPart(ctx, reply); err != nil {
				return err
			}
		default:
			if err := pi.SkipPart(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
