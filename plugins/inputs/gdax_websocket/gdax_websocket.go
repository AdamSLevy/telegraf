package gdaxWebsocket

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"

	ws "github.com/gorilla/websocket"
	gdax "github.com/preichenberger/go-gdax"
)

// GdaxWebsocket implements the telegraf.ServiceInput interface for collecting
// metrics from GDAX's websocket feed.
type GdaxWebsocket struct {
	FeedURL  string `toml:"feed_url"`
	Channels []*channelConfig

	numUsers int

	wsConns []*ws.Conn

	acc telegraf.Accumulator
}

type channelConfig struct {
	Channel string
	Pairs   []string

	UserName   string
	Key        string
	Secret     string
	Passphrase string
}

type subscription struct {
	Type     string                `json:"type"`
	Channels []channelSubscription `json:"channels"`

	Key        string `json:"key,omitempty"`
	Signature  string `json:"signature,omitempty"`
	Passphrase string `json:"passphrase,omitempty"`
	Timestamp  string `json:"timestamp,omitempty"`
}

type channelSubscription struct {
	Name  string   `json:"name"`
	Pairs []string `json:"product_ids"`
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
  #  key =      "$JOHN_GDAX_KEY"
  #  secret =   "$JOHN_GDAX_SECRET"
  #  password = "$JOHN_GDAX_PASSWORD"
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

	subs, err := gx.generateSubscriptions()
	if err != nil {
		return err
	}

	gx.acc = acc
	for _, sub := range subs {
		wsDialer := ws.Dialer{EnableCompression: true}
		wsConn, _, err := wsDialer.Dial(gx.FeedURL, nil)
		if err != nil {
			return err
		}
		gx.wsConns = append(gx.wsConns, wsConn)
		if err := wsConn.WriteJSON(sub); err != nil {
			gx.Stop()
			return err
		}
		go gx.listen(wsConn)
	}

	return nil
}

func (gx *GdaxWebsocket) listen(wsConn *ws.Conn) {
	message := gdax.Message{}
	for true {
		if err := wsConn.ReadJSON(&message); err != nil {
			log.Println(err.Error())
			break
		}
		fmt.Println(message)
	}
}

// Stop shuts down the running go routing and stops the service
func (gx *GdaxWebsocket) Stop() {
	for _, wsConn := range gx.wsConns {
		wsConn.Close()
	}
}

func (gx *GdaxWebsocket) validateConfig() error {
	if len(gx.FeedURL) == 0 {
		return fmt.Errorf("not specified: feed_url")
	}

	if len(gx.Channels) == 0 {
		return fmt.Errorf("no channels specified")
	}

	users := make(map[string]interface{})
	userKeys := make(map[string]interface{})
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
			if len(c.Key) > 0 || len(c.Secret) > 0 || len(c.Passphrase) > 0 {
				return fmt.Errorf(
					"cannot specify API credentials for '%s' channel",
					c.Channel)
			}
		case "user":
			if len(c.UserName) == 0 {
				return fmt.Errorf("no user_name for 'user' channel")
			}
			if len(c.Key) == 0 {
				return fmt.Errorf("no key specified for user '%s'",
					c.UserName)
			}
			if len(c.Secret) == 0 {
				return fmt.Errorf("no secret specified for user '%s'",
					c.UserName)
			}
			if len(c.Passphrase) == 0 {
				return fmt.Errorf("no passphrase specified for user '%s'",
					c.UserName)
			}
			if _, ok := users[c.UserName]; ok {
				return fmt.Errorf("user_name should be unique")
			}
			users[c.UserName] = true
			if _, ok := userKeys[c.Key]; ok {
				return fmt.Errorf("key should be unique")
			}
			userKeys[c.Key] = true
		default:
			return fmt.Errorf("invalid channel: '%s'", c.Channel)
		}
	}

	gx.numUsers = len(users)
	return nil
}

func (gx *GdaxWebsocket) generateSubscriptions() ([]subscription, error) {
	numSubs := 1
	if gx.numUsers > 1 {
		numSubs += gx.numUsers - 1
	}
	subs := make([]subscription, numSubs)
	subID := 0
	for _, channel := range gx.Channels {
		subs[subID].Type = "subscribe"
		subs[subID].Channels = append(subs[subID].Channels, channelSubscription{
			Name:  channel.Channel,
			Pairs: channel.Pairs,
		})
		if channel.Channel == "user" {
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			auth, err := gdax.NewClient(
				channel.Secret, channel.Key, channel.Passphrase).
				Headers("GET", "/users/self/verify", timestamp, "")
			if err != nil {
				return nil, err
			}
			subs[subID].Key = channel.Key
			subs[subID].Passphrase = channel.Passphrase
			subs[subID].Timestamp = timestamp
			subs[subID].Signature = auth["CB-ACCESS-SIGN"]
			subID++
		}
	}

	return subs, nil
}

func init() {
	inputs.Add("gdax_websocket", func() telegraf.Input { return &GdaxWebsocket{} })
}
