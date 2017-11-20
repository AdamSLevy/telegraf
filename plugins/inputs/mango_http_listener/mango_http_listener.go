package mango_http_listener

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	//"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs"
	//json_parser "github.com/influxdata/telegraf/plugins/parsers/json"
	"github.com/influxdata/telegraf/selfstat"
)

const (
	// DEFAULT_MAX_BODY_SIZE is the default maximum request body size, in bytes.
	// if the request body is over this size, we will return an HTTP 413 error.
	// 500 MB
	DEFAULT_MAX_BODY_SIZE = 500 * 1024 * 1024

	// MAX_LINE_SIZE is the maximum size, in bytes, that can be allocated for
	// a single InfluxDB point.
	// 64 KB
	DEFAULT_MAX_LINE_SIZE = 64 * 1024
)

type MangoHTTPListener struct {
	ServiceAddress string
	ReadTimeout    internal.Duration
	WriteTimeout   internal.Duration
	MaxBodySize    int64
	MaxLineSize    int
	Port           int

	TlsAllowedCacerts []string
	TlsCert           string
	TlsKey            string

	mu sync.Mutex
	wg sync.WaitGroup

	listener net.Listener

	acc  telegraf.Accumulator
	pool *pool

	BytesRecv       selfstat.Stat
	RequestsServed  selfstat.Stat
	WritesServed    selfstat.Stat
	QueriesServed   selfstat.Stat
	PingsServed     selfstat.Stat
	RequestsRecv    selfstat.Stat
	WritesRecv      selfstat.Stat
	QueriesRecv     selfstat.Stat
	PingsRecv       selfstat.Stat
	NotFoundsServed selfstat.Stat
	BuffersCreated  selfstat.Stat
}

const sampleConfig = `
  ## Address and port to host HTTP listener on
  service_address = ":8186"

  ## maximum duration before timing out read of the request
  read_timeout = "10s"
  ## maximum duration before timing out write of the response
  write_timeout = "10s"

  ## Maximum allowed http request body size in bytes.
  ## 0 means to use the default of 536,870,912 bytes (500 mebibytes)
  max_body_size = 0

  ## Maximum line size allowed to be sent in bytes.
  ## 0 means to use the default of 65536 bytes (64 kibibytes)
  max_line_size = 0

  ## Set one or more allowed client CA certificate file names to 
  ## enable mutually authenticated TLS connections
  tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

  ## Add service certificate and key
  tls_cert = "/etc/telegraf/cert.pem"
  tls_key = "/etc/telegraf/key.pem"
`

func (h *MangoHTTPListener) SampleConfig() string {
	return sampleConfig
}

func (h *MangoHTTPListener) Description() string {
	return "Influx HTTP write listener"
}

func (h *MangoHTTPListener) Gather(_ telegraf.Accumulator) error {
	h.BuffersCreated.Set(h.pool.ncreated())
	return nil
}

// Start starts the http listener service.
func (h *MangoHTTPListener) Start(acc telegraf.Accumulator) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	tags := map[string]string{
		"address": h.ServiceAddress,
	}
	h.BytesRecv = selfstat.Register("mango_http_listener", "bytes_received", tags)
	h.RequestsServed = selfstat.Register("mango_http_listener", "requests_served", tags)
	h.WritesServed = selfstat.Register("mango_http_listener", "writes_served", tags)
	h.QueriesServed = selfstat.Register("mango_http_listener", "queries_served", tags)
	h.PingsServed = selfstat.Register("mango_http_listener", "pings_served", tags)
	h.RequestsRecv = selfstat.Register("mango_http_listener", "requests_received", tags)
	h.WritesRecv = selfstat.Register("mango_http_listener", "writes_received", tags)
	h.QueriesRecv = selfstat.Register("mango_http_listener", "queries_received", tags)
	h.PingsRecv = selfstat.Register("mango_http_listener", "pings_received", tags)
	h.NotFoundsServed = selfstat.Register("mango_http_listener", "not_founds_served", tags)
	h.BuffersCreated = selfstat.Register("mango_http_listener", "buffers_created", tags)

	if h.MaxBodySize == 0 {
		h.MaxBodySize = DEFAULT_MAX_BODY_SIZE
	}
	if h.MaxLineSize == 0 {
		h.MaxLineSize = DEFAULT_MAX_LINE_SIZE
	}

	if h.ReadTimeout.Duration < time.Second {
		h.ReadTimeout.Duration = time.Second * 10
	}
	if h.WriteTimeout.Duration < time.Second {
		h.WriteTimeout.Duration = time.Second * 10
	}

	h.acc = acc
	h.pool = NewPool(200, h.MaxLineSize)

	tlsConf := h.getTLSConfig()

	server := &http.Server{
		Addr:         h.ServiceAddress,
		Handler:      h,
		ReadTimeout:  h.ReadTimeout.Duration,
		WriteTimeout: h.WriteTimeout.Duration,
		TLSConfig:    tlsConf,
	}

	var err error
	var listener net.Listener
	if tlsConf != nil {
		listener, err = tls.Listen("tcp", h.ServiceAddress, tlsConf)
	} else {
		listener, err = net.Listen("tcp", h.ServiceAddress)
	}
	if err != nil {
		return err
	}
	h.listener = listener
	h.Port = listener.Addr().(*net.TCPAddr).Port

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		server.Serve(h.listener)
	}()

	log.Printf("I! Started HTTP listener service on %s\n", h.ServiceAddress)

	return nil
}

// Stop cleans up all resources
func (h *MangoHTTPListener) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.listener.Close()
	h.wg.Wait()

	log.Println("I! Stopped HTTP listener service on ", h.ServiceAddress)
}

func (h *MangoHTTPListener) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	h.RequestsRecv.Incr(1)
	defer h.RequestsServed.Incr(1)
	switch req.URL.Path {
	case "/write":
		h.WritesRecv.Incr(1)
		defer h.WritesServed.Incr(1)
		h.serveWrite(res, req)
	case "/query":
		h.QueriesRecv.Incr(1)
		defer h.QueriesServed.Incr(1)
		// Deliver a dummy response to the query endpoint, as some InfluxDB
		// clients test endpoint availability with a query
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("X-Influxdb-Version", "1.0")
		res.WriteHeader(http.StatusOK)
		res.Write([]byte("{\"results\":[]}"))
	case "/ping":
		h.PingsRecv.Incr(1)
		defer h.PingsServed.Incr(1)
		// respond to ping requests
		res.WriteHeader(http.StatusNoContent)
	default:
		defer h.NotFoundsServed.Incr(1)
		// Don't know how to respond to calls to other endpoints
		http.NotFound(res, req)
	}
}

func (h *MangoHTTPListener) serveWrite(res http.ResponseWriter, req *http.Request) {
	// Check that the content length is not too large for us to handle.
	if req.ContentLength > h.MaxBodySize {
		tooLarge(res)
		return
	}
	now := time.Now()

	precision := req.URL.Query().Get("precision")

	// Handle gzip request bodies
	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		var err error
		body, err = gzip.NewReader(req.Body)
		defer body.Close()
		if err != nil {
			log.Println("E! " + err.Error())
			badRequest(res)
			return
		}
	}
	body = http.MaxBytesReader(res, body, h.MaxBodySize)

	var return400 bool
	var hangingBytes bool
	buf := h.pool.get()
	defer h.pool.put(buf)
	bufStart := 0
	for {
		n, err := io.ReadFull(body, buf[bufStart:])
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			log.Println("E! " + err.Error())
			// problem reading the request body
			badRequest(res)
			return
		}
		h.BytesRecv.Incr(int64(n))

		if err == io.EOF {
			if return400 {
				badRequest(res)
			} else {
				res.WriteHeader(http.StatusNoContent)
			}
			return
		}

		if hangingBytes {
			i := bytes.IndexByte(buf, '\n')
			if i == -1 {
				// still didn't find a newline, keep scanning
				continue
			}
			// rotate the bit remaining after the first newline to the front of the buffer
			i++ // start copying after the newline
			bufStart = len(buf) - i
			if bufStart > 0 {
				copy(buf, buf[i:])
			}
			hangingBytes = false
			continue
		}

		if err == io.ErrUnexpectedEOF {
			// finished reading the request body
			if err := h.parse(buf[:n+bufStart], now, precision); err != nil {
				log.Println("E! " + err.Error())
				return400 = true
			}
			if return400 {
				badRequest(res)
			} else {
				res.WriteHeader(http.StatusNoContent)
			}
			return
		}

		// if we got down here it means that we filled our buffer, and there
		// are still bytes remaining to be read. So we will parse up until the
		// final newline, then push the rest of the bytes into the next buffer.
		i := bytes.LastIndexByte(buf, '\n')
		if i == -1 {
			// drop any line longer than the max buffer size
			log.Printf("E! http_listener_json received a single line longer than the maximum of %d bytes",
				len(buf))
			hangingBytes = true
			return400 = true
			bufStart = 0
			continue
		}
		if err := h.parse(buf[:i+1], now, precision); err != nil {
			log.Println("E! " + err.Error())
			return400 = true
		}
		// rotate the bit remaining after the last newline to the front of the buffer
		i++ // start copying after the newline
		bufStart = len(buf) - i
		if bufStart > 0 {
			copy(buf, buf[i:])
		}
	}
}

func (h *MangoHTTPListener) parse(b []byte, t time.Time, precision string) error {
	// Data format: {"data": "10.000@1234456667", "data2":"144@12345566"}

	// Unmarshall two copies of the JSON, one of the timestamp and one of the value
	regValues := regexp.MustCompile(`"[\d\.]+@`) // Matches values
	regTimes := regexp.MustCompile(`@\d+"`)      // Matches timestamps

	valuesBytes := []byte(regTimes.ReplaceAllString(string(b), `"`))
	timesBytes := []byte(regValues.ReplaceAllString(string(b), `"`))

	var valuesJSON map[string]interface{}
	var timesJSON map[string]interface{}
	json.Unmarshal(valuesBytes, &valuesJSON)
	json.Unmarshal(timesBytes, &timesJSON)
	log.Printf("original JSON: %s\n", b)
	log.Printf("values JSON:   %s\n", valuesJSON)
	log.Printf("times JSON:    %s\n", timesJSON)

	// Extract Tags

	// All values and times are strings, convert to correct type
	for k, v := range valuesJSON {
		if v, ok := v.(string); ok {
			if x, err := strconv.ParseBool(v); err == nil {
				valuesJSON[k] = x
			} else if x, err := strconv.ParseInt(v, 10, 64); err == nil {
				valuesJSON[k] = x
			} else if x, err := strconv.ParseFloat(v, 64); err == nil {
				valuesJSON[k] = x
			}
		}
		if t, ok := timesJSON[k]; ok {
			if t, ok := t.(string); ok {
				if x, err := strconv.ParseInt(t, 10, 64); err == nil {
					timesJSON[k] =
						time.Unix(0, x*int64(time.Millisecond))
				}
			}
		}
		log.Printf("key: \"%s\", value: %v, type: %T, time: %v, type: %T \n",
			k, valuesJSON[k], valuesJSON[k], timesJSON[k], timesJSON[k])
	}

	//metrics := make([]telegraf.Metric, 0)
	//metric, err := metric.New("mango_http_listener", nil, f.Fields, time.Now().UTC())
	////f := json_parser.JSONFlattener{}
	//err := f.FullFlattenJSON("", jsonOut, true, true)
	//if err != nil {
	//	log.Println(err)
	//}
	//log.Println("fields: ", f.Fields)
	//if err != nil {
	//	return err
	//}
	//metrics = append(metrics, metric)

	//log.Println("metrics: ", metrics)

	//for _, m := range metrics {
	//	fields := make(map[string]interface{})
	//	for k, v := range m.Fields() {
	//		fields[k] = v
	//	}
	//	h.acc.AddFields(m.Name(), fields, m.Tags(), m.Time())
	//}

	return nil //err
}

func tooLarge(res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("X-Influxdb-Version", "1.0")
	res.WriteHeader(http.StatusRequestEntityTooLarge)
	res.Write([]byte(`{"error":"http: request body too large"}`))
}

func badRequest(res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("X-Influxdb-Version", "1.0")
	res.WriteHeader(http.StatusBadRequest)
	res.Write([]byte(`{"error":"http: bad request"}`))
}

func (h *MangoHTTPListener) getTLSConfig() *tls.Config {
	tlsConf := &tls.Config{
		InsecureSkipVerify: false,
		Renegotiation:      tls.RenegotiateNever,
	}

	if len(h.TlsCert) == 0 || len(h.TlsKey) == 0 {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(h.TlsCert, h.TlsKey)
	if err != nil {
		return nil
	}
	tlsConf.Certificates = []tls.Certificate{cert}

	if h.TlsAllowedCacerts != nil {
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
		clientPool := x509.NewCertPool()
		for _, ca := range h.TlsAllowedCacerts {
			c, err := ioutil.ReadFile(ca)
			if err != nil {
				continue
			}
			clientPool.AppendCertsFromPEM(c)
		}
		tlsConf.ClientCAs = clientPool
	}

	return tlsConf
}

func init() {
	inputs.Add("http_listener_json", func() telegraf.Input {
		return &MangoHTTPListener{
			ServiceAddress: ":8186",
		}
	})
}
