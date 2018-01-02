package gdaxWebsocket

import (
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
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
	Pairs    []string
	Channels []channelConfig

	userNamesByKey map[string]string

	wg    sync.WaitGroup
	conns []conn

	acc telegraf.Accumulator
}

type channelConfig struct {
	Channel string   `json:"name"`
	Pairs   []string `json:"product_ids,omitempty"`

	UserName   string `json:"-"`
	Key        string `json:"-"`
	Secret     string `json:"-"`
	Passphrase string `json:"-"`
}

type subscribeRequest struct {
	Type     string          `json:"type"`
	Pairs    []string        `json:"product_ids,omitempty"`
	Channels []channelConfig `json:"channels"`

	Key        string `json:"key,omitempty"`
	Signature  string `json:"signature,omitempty"`
	Passphrase string `json:"passphrase,omitempty"`
	Timestamp  string `json:"timestamp,omitempty"`
}

type subscribeResponse struct {
	subscribeRequest
}

// Description prints a short description
func (gx *GdaxWebsocket) Description() string {
	return "Subscribes to channels on GDAX's websocket feed"
}

// SampleConfig prints an example config section
func (gx *GdaxWebsocket) SampleConfig() string {
	return `
  ## GDAX websocket feed URL. 
  # feed_url = "wss://ws-feed.gdax.com"	# Default
  pairs = [ "ETH-USD", "BTC-USD" ]	# These pairs will apply globally to all channels

  ## Channels to subscribe to. Only ticker, level2, and user are supported.
  ## See https://docs.gdax.com/#overview for channel details.
  [[ inputs.gdax_websocket.channels ]]
    channel = "ticker"		# Required
    ## Additional channel specific pairs.
    pairs = [ "ETH-USD", "ETH-BTC" ] 	# Redundant pairs are OK

  #[[ inputs.gdax_websocket.channels ]]
  #  channel = "level2"
  #  ## Channel specific pairs can be omitted only if 'pairs' was define globally

  #[[ inputs.gdax_websocket.channels ]]
  #  channel = "user"
  #  user_name = "John" 		# Required for "user" channel
  #					## creates tag user="John"
  #  pairs = [ "LTC-USD" ]
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

var dial = wsDial

func wsDial(feedURL string) (conn, error) {
	dialer := ws.Dialer{EnableCompression: true}
	wsConn, _, err := dialer.Dial(feedURL, nil)
	return wsConn, err
}

type conn interface {
	WriteJSON(interface{}) error
	ReadJSON(interface{}) error
	Close() error
}

// Start validates the config, opens the websocket, subscribes to feeds, and
// then launches a go routine to process the data stream.
func (gx *GdaxWebsocket) Start(acc telegraf.Accumulator) error {
	if err := gx.validateConfig(); err != nil {
		return err
	}

	gx.acc = acc

	subs := gx.generateSubscribeRequests()

	for _, req := range subs {
		wsConn, err := dial(gx.FeedURL)
		if err != nil {
			return err
		}
		gx.conns = append(gx.conns, wsConn)
		if err := wsConn.WriteJSON(req); err != nil {
			gx.Stop()
			return err
		}
		var res subscribeResponse
		if err := wsConn.ReadJSON(&res); err != nil {
			gx.Stop()
			return err
		}

		if err := validateSubscribeResponse(req, res); err != nil {
			gx.Stop()
			return err
		}

		gx.wg.Add(1)
		go gx.listen(wsConn)
	}

	return nil
}

var lPrintln = log.Println

func (gx *GdaxWebsocket) listen(c conn) {
	defer gx.wg.Done()
	msg := gdax.Message{}
	for {
		if err := c.ReadJSON(&msg); err != nil {
			lPrintln(err)
			break
		}
		lPrintln(msg)
	}
}

// Stop shuts down the running go routine and stops the service
func (gx *GdaxWebsocket) Stop() {
	for _, c := range gx.conns {
		c.Close()
	}
	gx.wg.Wait()
	gx.conns = nil
}

func (gx *GdaxWebsocket) validateConfig() error {
	if len(gx.FeedURL) == 0 {
		return fmt.Errorf("not specified: feed_url")
	}

	if len(gx.Channels) == 0 {
		return fmt.Errorf("no channels specified")
	}

	globalPairs := make(map[string]bool)
	var pairs []string
	for _, pair := range gx.Pairs {
		pair = strings.ToUpper(pair)
		if _, ok := globalPairs[pair]; !ok {
			globalPairs[pair] = true
			pairs = append(pairs, pair)
		}
	}
	gx.Pairs = pairs

	users := make(map[string]bool)
	userNamesByKey := make(map[string]string)
	var ticker, level2 bool
	for i, c := range gx.Channels {
		if len(c.Channel) == 0 {
			return fmt.Errorf("not specified: channel")
		}

		if len(gx.Pairs)+len(c.Pairs) == 0 {
			return fmt.Errorf("no pairs specified for '%s' channel",
				c.Channel)
		}

		channelPairs := make(map[string]bool)
		pairs = nil
		for _, pair := range c.Pairs {
			pair = strings.ToUpper(pair)
			if _, ok := globalPairs[pair]; !ok {
				if _, ok := channelPairs[pair]; !ok {
					channelPairs[pair] = true
					pairs = append(pairs, pair)
				}
			}
		}
		gx.Channels[i].Pairs = pairs

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
	hasUser := false
	for _, channel := range gx.Channels {
		if channel.Channel == "user" {
			if hasUser {
				// Only one authenticated user per websocket
				// connection. Move on to next subscription.
				subID++
				hasUser = false
			} else {
				hasUser = true
			}
			// Add credentials to subscribeRequest
			subs[subID].Key = channel.Key
			subs[subID].Passphrase = channel.Passphrase
			timestamp, signature := getSignature(channel.Secret, channel.Key,
				channel.Passphrase)
			subs[subID].Timestamp = timestamp
			subs[subID].Signature = signature
		}
		subs[subID].Channels = append(subs[subID].Channels, channel)
		//	fmt.Printf("len(subs[%v].Channels) %v\n",
		//		subID, len(subs[subID].Channels))

	}

	for i, _ := range subs {
		subs[i].Type = "subscribe"
		subs[i].Pairs = gx.Pairs
	}

	return subs
}

func validateSubscribeResponse(req subscribeRequest, res subscribeResponse) error {
	if res.Type != "subscriptions" {
		return fmt.Errorf("invalid GDAX response: 'type' != 'subscriptions'")
	}
	if len(res.Channels) != len(req.Channels) {
		return fmt.Errorf("invalid GDAX response: 'channels' length differs")
	}

	for _, resChannel := range res.Channels {
		match := false
		for _, reqChannel := range req.Channels {
			if resChannel.Channel == reqChannel.Channel {
				if err := validateChannelPairs(reqChannel.Pairs,
					req.Pairs, resChannel.Pairs); err != nil {
					return err
				}
				match = true
			}
		}
		if !match {
			return fmt.Errorf("invalid GDAX response: channel mismatch")
		}

	}
	return nil
}

func validateChannelPairs(reqChannelPairs []string, reqGlobalPairs []string,
	resChannelPairs []string) error {
	if len(reqChannelPairs)+len(reqGlobalPairs) != len(resChannelPairs) {
		return fmt.Errorf("invalid GDAX response: pair length mismatch")
	}
	for _, resPair := range resChannelPairs {
		pairMatch := false
		for _, reqPair := range reqChannelPairs {
			if resPair == reqPair {
				pairMatch = true
				break
			}
		}
		if pairMatch {
			continue
		}
		for _, reqPair := range reqGlobalPairs {
			if resPair == reqPair {
				pairMatch = true
				break
			}
		}
		if pairMatch {
			continue
		}
		return fmt.Errorf("invalid GDAX response: pair mismatch")
	}

	return nil
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

func newTelegrafInput() telegraf.Input {
	return &GdaxWebsocket{}
}

func init() {
	inputs.Add("gdax_websocket", newTelegrafInput)
}
