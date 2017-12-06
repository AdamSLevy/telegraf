package gdaxWebsocket

import (
	"encoding/base64"
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
	Channels []channelConfig

	userNamesByKey map[string]string

	wsConns []*ws.Conn

	acc telegraf.Accumulator
}

type channelConfig struct {
	Channel string   `json:"name"`
	Pairs   []string `json:"product_ids"`

	UserName   string `json:"-"`
	Key        string `json:"-"`
	Secret     string `json:"-"`
	Passphrase string `json:"-"`
}

type subscribeRequest struct {
	Type     string          `json:"type"`
	Channels []channelConfig `json:"channels"`

	Key        string `json:"key,omitempty"`
	Signature  string `json:"signature,omitempty"`
	Passphrase string `json:"passphrase,omitempty"`
	Timestamp  string `json:"timestamp,omitempty"`
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

	gx.acc = acc

	subs := gx.generateSubscribeRequests()

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
	userNamesByKey := make(map[string]string)
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

			if _, e := base64.StdEncoding.DecodeString(c.Key); e != nil {
				return fmt.Errorf("non-base64 key")
			}
			if len(c.Secret) == 0 {
				return fmt.Errorf("no secret specified for user '%s'",
					c.UserName)
			}
			if _, e := base64.StdEncoding.DecodeString(c.Secret); e != nil {
				return fmt.Errorf("non-base64 secret")
			}
			if len(c.Passphrase) == 0 {
				return fmt.Errorf("no passphrase specified for user '%s'",
					c.UserName)
			}
			if _, ok := users[c.UserName]; ok {
				return fmt.Errorf("user_name should be unique")
			}
			users[c.UserName] = true
			if _, ok := userNamesByKey[c.Key]; ok {
				return fmt.Errorf("key should be unique")
			}
			userNamesByKey[c.Key] = c.UserName
		default:
			return fmt.Errorf("invalid channel: '%s'", c.Channel)
		}
	}

	gx.userNamesByKey = userNamesByKey
	return nil
}

// generateSubscribeRequests generates a slice of subscribeRequest objects from
// the GdaxWebsockets channelConfigs. Separate websocket connections, and thus,
// separate subscribeRequests are required for each user channel. However, the
// ticker and level2 channels can be on the same websocket connection together
// and with a user channel.
func (gx *GdaxWebsocket) generateSubscribeRequests() []subscribeRequest {
	numSubs := 1
	numUsers := len(gx.userNamesByKey)
	if numUsers > 1 {
		numSubs += numUsers - 1
	}
	subs := make([]subscribeRequest, numSubs)
	subID := 0
	for _, channel := range gx.Channels {
		subs[subID].Type = "subscribe"
		subs[subID].Channels = append(subs[subID].Channels, channel)

		if channel.Channel == "user" {
			// Add credentials to subscribeRequest
			subs[subID].Key = channel.Key
			subs[subID].Passphrase = channel.Passphrase
			timestamp, signature := getSignature(channel.Secret, channel.Key,
				channel.Passphrase)
			subs[subID].Timestamp = timestamp
			subs[subID].Signature = signature
			// Only one authenticated user per websocket
			// connection. Move on to next subscription.
			subID++
		}
	}

	return subs
}

// getSignature returns the timestamp and signature for a websocket connection
// given the user's secret, key, and passphrase.
func getSignature(secret, key, passphrase string) (timestamp, signature string) {
	timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	auth, _ := gdax.NewClient(secret, key, passphrase).
		Headers("GET", "/users/self/verify", timestamp, "")
	signature = auth["CB-ACCESS-SIGN"]
	return

}

func init() {
	inputs.Add("gdax_websocket", func() telegraf.Input { return &GdaxWebsocket{} })
}
