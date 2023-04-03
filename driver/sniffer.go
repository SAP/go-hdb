package driver

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/unicode/cesu8"
)

// A Sniffer is a simple proxy for logging hdb protocol requests and responses.
type Sniffer struct {
	logger *log.Logger
	conn   net.Conn
	dbConn net.Conn
}

// NewSniffer creates a new sniffer instance. The conn parameter is the net.Conn connection, where the Sniffer
// is listening for hdb protocol calls. The dbAddr is the hdb host port address in "host:port" format.
func NewSniffer(conn net.Conn, dbConn net.Conn) *Sniffer {
	return &Sniffer{
		logger: log.New(os.Stdout, fmt.Sprintf("%s ", conn.RemoteAddr()), log.Ldate|log.Ltime),
		conn:   conn,
		dbConn: dbConn,
	}
}

func pipeData(wg *sync.WaitGroup, conn net.Conn, dbConn net.Conn, wr io.Writer) {
	defer wg.Done()

	mwr := io.MultiWriter(dbConn, wr)
	trd := io.TeeReader(conn, mwr)
	buf := make([]byte, 1000)

	var err error
	for err == nil {
		_, err = trd.Read(buf)
	}
}

func readMsg(prd p.Reader) error {
	// TODO complete for non generic parts, see internal/protocol/parts/newGenPartReader for details
	return prd.IterateParts(func(ph *p.PartHeader) {
	})
}

func logData(wg *sync.WaitGroup, prd p.Reader) {
	defer wg.Done()

	if err := prd.ReadProlog(); err != nil {
		panic(err)
	}

	var err error
	for err != io.EOF {
		err = readMsg(prd)
	}
}

// Run starts the protocol request and response logging.
func (s *Sniffer) Run() error {
	clientRd, clientWr := io.Pipe()
	dbRd, dbWr := io.Pipe()

	wg := &sync.WaitGroup{}

	wg.Add(4)
	go pipeData(wg, s.conn, s.dbConn, clientWr)
	go pipeData(wg, s.dbConn, s.conn, dbWr)

	pClientRd := p.NewClientReader(clientRd, s.logger, cesu8.DefaultDecoder)
	pDBRd := p.NewDBReader(dbRd, s.logger, cesu8.DefaultDecoder)

	go logData(wg, pClientRd)
	go logData(wg, pDBRd)

	wg.Wait()
	log.Println("end run")
	return nil
}
