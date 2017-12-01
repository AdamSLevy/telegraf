package gdax_websocket

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

type GdaxWebsocket struct {
	FeedURL  string `toml:"feed_url"`
	Channels []channel_t

	acc telegraf.Accumulator
}

type channel_t struct {
	Channel string
	Pairs   []string

	UserName    string
	Credentials map[string]string
}

func (sl *GdaxWebsocket) Description() string {
	return "Subscribes to channels on the GDAX websocket"
}

func (sl *GdaxWebsocket) SampleConfig() string {
	return `
  ## GDAX websocket feed URL. 
  feed_url = "wss://ws-feed.gdax.com"	# Required

  ## Channels to subscribe to. At least one is required.
  ## See https://docs.gdax.com/#overview for channel details.
  [[ inputs.gdax_websocket.channels ]]
    channel = "ticker"  	    # Required
    pairs = [ "ETH-USD" "BTC-USD" ] # At least one GDAX product pair is required

  #[[ inputs.gdax_websocket.channels ]]
  #  channel = "level2"
  #  pairs = [ "ETH-USD" ]

  #[[ inputs.gdax_websocket.channels ]]
  #  channel = "user"
  #  pairs = [ "ETH-USD" ]
  #  user_name = "John" 		# Required for "user" channel
  #  ## User Credentials Required for "user" channel
  #  ## Adding user config subscribes to additional "user" channels.
  #  ## Security Note: Do NOT set user credentials in telegraf config files
  #  ## 	       directly. Use environment variables to store credentials.
  #  ##		       With systemd services this can be achieved using the
  #  ##  	       'EnvironmentFile' variable. See man systemd.service
  #  [ inputs.gdax_websocket.channels.credentials ]
  #    secret =   "$JOHN_GDAX_SECRET"
  #    key =      "$JOHN_GDAX_KEY"
  #    password = "$JOHN_GDAX_PASSWORD"
`
}

func (sl *GdaxWebsocket) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (sl *GdaxWebsocket) SetParser(parser parsers.Parser) {
	sl.Parser = parser
}

func (sl *GdaxWebsocket) Start(acc telegraf.Accumulator) error {
	return nil
}

func (sl *GdaxWebsocket) Stop() {
}

func newGdaxWebsocket() *GdaxWebsocket {
	parser, _ := parsers.NewInfluxParser()

	return &GdaxWebsocket{
		Parser: parser,
	}
}

type unixCloser struct {
	path   string
	closer io.Closer
}

func (uc unixCloser) Close() error {
	err := uc.closer.Close()
	os.Remove(uc.path) // ignore error
	return err
}

func init() {
	inputs.Add("gdax_websocket", func() telegraf.Input { return newGdaxWebsocket() })
}
