package gdaxWebsocket

import (
	"fmt"
	//"log"
	"strings"
	//"net/url"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	//ws "github.com/gorilla/websocket"
)

// GdaxWebsocket implenets the telegraf.ServiceInput interface for collecting
// metrics from GDAX's webwocket feed.
type GdaxWebsocket struct {
	FeedURL  string `toml:"feed_url"`
	Channels []*channelConfig

	acc telegraf.Accumulator
}

type channelConfig struct {
	Channel string
	Pairs   []string

	UserName    string
	Credentials map[string]string
}

// Description prints a short description
func (gx *GdaxWebsocket) Description() string {
	return "Subscribes to channels on the GDAX websocket"
}

// SampleConfig prints an example config section
func (gx *GdaxWebsocket) SampleConfig() string {
	return `
  ## GDAX websocket feed URL. 
  feed_url = "wss://ws-feed.gdax.com"	# Required

  ## Channels to subscribe to. 
  ## See https://docs.gdax.com/#overview for channel details.
  [[ inputs.gdax_websocket.channels ]]
    channel = "ticker"  	    # Required
    pairs = [ "ETH-USD", "BTC-USD" ] # At least one GDAX product pair is required

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
  #  ##		       Remember to restrict permissions on the file to 600.
  #  [ inputs.gdax_websocket.channels.credentials ] # Required for "user" channel
  #    secret =   "$JOHN_GDAX_SECRET"
  #    key =      "$JOHN_GDAX_KEY"
  #    password = "$JOHN_GDAX_PASSWORD"
`
}

// Gather returns nil
func (gx *GdaxWebsocket) Gather(_ telegraf.Accumulator) error {
	return nil
}

// Start validates the config, opens the websocket, subscribes to feeds, and
// then launches a go routine to process the data stream.
func (gx *GdaxWebsocket) Start(acc telegraf.Accumulator) error {
	if err := gx.validateConfig(); err != nil {
		return err
	}

	gx.acc = acc

	return nil
}

// Stop shuts down the running go routing and stops the service
func (gx *GdaxWebsocket) Stop() {
}

func (gx *GdaxWebsocket) validateConfig() error {
	if len(gx.FeedURL) == 0 {
		return fmt.Errorf("not specified: feed_url")
	}

	if len(gx.Channels) == 0 {
		return fmt.Errorf("no channels specified")
	}

	users := make(map[string]interface{})
	var ticker, level2 bool
	for _, c := range gx.Channels {
		if len(c.Channel) == 0 {
			return fmt.Errorf("not specified: channel")
		}

		if len(c.Pairs) == 0 {
			return fmt.Errorf("no pairs specified for '%s' channel",
				c.Channel)
		}

		for i, pair := range c.Pairs {
			c.Pairs[i] = strings.ToUpper(pair)
		}

		var fall bool
		switch c.Channel {
		case "ticker":
			if ticker {
				return fmt.Errorf("channel 'ticker' declared twice")
			}
			ticker = true
			fall = true
			fallthrough
		case "level2":
			if !fall {
				if level2 {
					return fmt.Errorf(
						"channel 'level2' declared twice")
				}
				level2 = true
			}
			if len(c.UserName) > 0 {
				return fmt.Errorf(
					"cannot specify user_name for '%s' channel",
					c.Channel)
			}
			if len(c.Credentials) > 0 {
				return fmt.Errorf(
					"cannot specify credentials for '%s' channel",
					c.Channel)
			}
		case "user":
			if len(c.UserName) == 0 {
				return fmt.Errorf("no user_name for 'user' channel")
			}
			if _, ok := users[c.UserName]; ok {
				return fmt.Errorf("user_name should be unique")
			}
			users[c.UserName] = true
			if len(c.Credentials) == 0 {
				return fmt.Errorf("no credentials for user_name '%s'",
					c.UserName)
			}
			var key, secret, password bool
			for k, v := range c.Credentials {
				switch k {
				case "key":
					if len(v) > 0 {
						key = true
					}
				case "secret":
					if len(v) > 0 {
						secret = true
					}
				case "password":
					if len(v) > 0 {
						password = true
					}
				default:
					return fmt.Errorf(
						"invalid credentials option: %s", k)
				}
			}
			if !key {
				return fmt.Errorf("no key specified for user '%s'",
					c.UserName)
			}
			if !secret {
				return fmt.Errorf("no secret specified for user '%s'",
					c.UserName)
			}
			if !password {
				return fmt.Errorf("no password specified for user '%s'",
					c.UserName)
			}
		default:
			return fmt.Errorf("invalid channel: '%s'", c.Channel)
		}
	}

	return nil
}

func init() {
	inputs.Add("gdax_websocket", func() telegraf.Input { return &GdaxWebsocket{} })
}
