package gdaxWebsocket

import (
	"errors"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/gdax_websocket/mocks"
	"github.com/influxdata/telegraf/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var tickerChannelConfig = channelConfig{
	Channel: "ticker",
	Pairs:   []string{"ETH-USD"},
}

var level2ChannelConfig = channelConfig{
	Channel: "level2",
	Pairs:   []string{"BTC-USD", "ETH-USD"},
}

var userChannelConfigs = []channelConfig{
	{
		Channel:    "user",
		Pairs:      []string{"BTC-USD", "ETH-USD"},
		UserName:   "John Smith",
		Key:        "a531631f192bf4778a19fbfbfae237a5",
		Secret:     "a4446242132b34778a19fbfbfae237a5",
		Passphrase: "passphrase",
	},
	{
		Channel:    "user",
		Pairs:      []string{"BTC-USD", "ETH-USD"},
		UserName:   "Jane Smith",
		Key:        "b323431f192bf4778a19fbfbfbb237b5",
		Secret:     "a532631f1923f4778a194bfbfae337a5",
		Passphrase: "passphrase",
	},
}

var validTestGdaxWebsocket = GdaxWebsocket{
	FeedURL: "wss://ws-feed.gdax.com",
	Channels: append([]channelConfig{
		tickerChannelConfig,
		level2ChannelConfig,
	}, userChannelConfigs...),
}

// This is a stupid test and I know it.
func TestServiceInput(t *testing.T) {
	gx := &GdaxWebsocket{}
	acc := &testutil.Accumulator{}
	assert := assert.New(t)

	assert.Implements((*telegraf.ServiceInput)(nil), gx,
		"implement telegraf.ServiceInput")

	assert.Implements((*telegraf.Input)(nil), newTelegrafInput(),
		"implement telegraf.Input")

	assert.NotEmpty(gx.SampleConfig(), "Sample Config")

	assert.NotEmpty(gx.Description(), "Description")

	assert.Nil(gx.Gather(acc), "Gather should return nil")
}

func TestValidateConfig(t *testing.T) {
	gx := validTestGdaxWebsocket
	assert := assert.New(t)

	gx.FeedURL = ""
	assert.Error(gx.validateConfig(), "empty FeedURL")

	gx = validTestGdaxWebsocket
	gx.Channels = nil
	assert.Error(gx.validateConfig(), "empty Channels")

	gx.Channels = append([]channelConfig(nil), validTestGdaxWebsocket.Channels...)
	gx.Channels[0].Channel = ""
	assert.Error(gx.validateConfig(), "empty Channel in Channels")

	gx.Channels = append([]channelConfig(nil), validTestGdaxWebsocket.Channels...)
	gx.Channels[0].Pairs = nil
	assert.Error(gx.validateConfig(), "empty Pairs")

	gx.Pairs = []string{"eth-usd", "BTC-ETH", "ETH-USD"}
	gx.Channels[1].Pairs = []string{"btc-usd", "btc-usd", "BTC-ETH", "ETH-USD"}
	assert.NoError(gx.validateConfig(), "global Pairs, empty channel Pairs")
	assert.Len(gx.Pairs, 2, "remove duplicate global pair")
	assert.Equal(gx.Pairs[0], "ETH-USD", "ToUpper on global pairs")
	assert.Len(gx.Channels[0].Pairs, 0, "no channel specific pairs")
	assert.Len(gx.Channels[1].Pairs, 1, "remove duplicate channel pairs")
	assert.Equal(gx.Channels[1].Pairs[0], "BTC-USD", "ToUpper on channel pairs")

	gx.Channels = append([]channelConfig(nil), validTestGdaxWebsocket.Channels...)
	gx.Channels[0].Channel = "invalid"
	assert.Error(gx.validateConfig(), "invalid Channel")

	testChannels := [][]channelConfig{{tickerChannelConfig}, {level2ChannelConfig}}
	for _, channels := range testChannels {
		gx.Channels = append([]channelConfig(nil), channels...)
		channel := channels[0].Channel
		assert.NoError(gx.validateConfig(), "channel '%s'", channel)
		assert.Len(gx.userNamesByKey, 0, "userNamesByKey")

		channelConfigCopy := gx.Channels[0]
		gx.Channels = append(gx.Channels, channelConfigCopy)
		assert.Errorf(gx.validateConfig(),
			"multiple '%s' channels", channel)

		gx.Channels = append([]channelConfig(nil), channels...)
		gx.Channels[0].UserName = "John Smith"
		assert.Errorf(gx.validateConfig(),
			"non-empty UserName for channel '%s'", channel)

		gx.Channels = append([]channelConfig(nil), channels...)
		gx.Channels[0].Key = "api key"
		assert.Errorf(gx.validateConfig(),
			"non-empty Key for channel '%s'", channel)

		gx.Channels = append([]channelConfig(nil), channels...)
		gx.Channels[0].Secret = "api secret"
		assert.Errorf(gx.validateConfig(),
			"non-empty Secret for channel '%s'", channel)

		gx.Channels = append([]channelConfig(nil), channels...)
		gx.Channels[0].Passphrase = "api passphrase"
		assert.Errorf(gx.validateConfig(),
			"non-empty Passphrase for channel '%s'", channel)
	}

	gx.Channels = append([]channelConfig(nil), userChannelConfigs[:1]...)
	gx.Channels[0].UserName = ""
	assert.Error(gx.validateConfig(), "empty UserName")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs[:1]...)
	gx.Channels[0].Key = ""
	assert.Error(gx.validateConfig(), "empty Key")

	gx.Channels[0].Key = "not valid base64 !@#$"
	assert.Error(gx.validateConfig(), "non-base64 Key")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs[:1]...)
	gx.Channels[0].Secret = ""
	assert.Error(gx.validateConfig(), "empty Secret")

	gx.Channels[0].Secret = "not valid base64 !@#$"
	assert.Error(gx.validateConfig(), "non-base64 Secret")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs[:1]...)
	gx.Channels[0].Passphrase = ""
	assert.Error(gx.validateConfig(), "empty Passphrase")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs[:1]...)
	assert.NoError(gx.validateConfig(), "valid 'user' channel")
	assert.Len(gx.userNamesByKey, 1, "userNamesByKey")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs...)
	gx.Channels[0].UserName = gx.Channels[1].UserName
	assert.Error(gx.validateConfig(), "duplicate UserName")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs...)
	gx.Channels[0].Key = gx.Channels[1].Key
	assert.Error(gx.validateConfig(), "duplicate Key")

	gx.Channels = append([]channelConfig(nil), userChannelConfigs...)
	assert.NoError(gx.validateConfig(), "multiple valid 'user' channels")
	assert.Len(gx.userNamesByKey, 2, "userNamesByKey")
}

func TestGenerateSubscribeRequests(t *testing.T) {
	require := require.New(t)
	gx := validTestGdaxWebsocket

	gx.Channels = append([]channelConfig{tickerChannelConfig, level2ChannelConfig},
		userChannelConfigs...)
	require.NoError(gx.validateConfig(), "two users")
	testSubscribeRequests(t, gx.generateSubscribeRequests(), 2)

	// Put one user channelConfig first
	gx.Channels = append(gx.Channels[len(gx.Channels)-1:len(gx.Channels)],
		gx.Channels[:len(gx.Channels)-1]...)
	require.NoError(gx.validateConfig(), "a user first")
	testSubscribeRequests(t, gx.generateSubscribeRequests(), 2)

	// Put both user channelConfigs first
	gx.Channels = append(gx.Channels[len(gx.Channels)-1:len(gx.Channels)],
		gx.Channels[:len(gx.Channels)-1]...)
	require.NoError(gx.validateConfig(), "both users first")
	testSubscribeRequests(t, gx.generateSubscribeRequests(), 2)

	gx.Channels = gx.Channels[1:len(gx.Channels)]
	require.NoError(gx.validateConfig(), "one user")
	testSubscribeRequests(t, gx.generateSubscribeRequests(), 1)
}

func testSubscribeRequests(t *testing.T, subs []subscribeRequest, numExpected int) {
	assert := assert.New(t)
	assert.Len(subs, numExpected,
		"config with %v users", numExpected)
	for _, sub := range subs {
		assert.Equal(sub.Type, "subscribe")
		assert.NotEmpty(sub.Channels, "Channels")
		assert.NotEmpty(sub.Key, "config with a user should have a non-empty Key")
		assert.NotEmpty(sub.Signature,
			"config with a user should have a non-empty Signature")
		assert.NotEmpty(sub.Passphrase,
			"config with a user should have a non-empty Passphrase")
		assert.NotEmpty(sub.Timestamp,
			"config with a user should have a non-empty Timestamp")
	}

}

func TestStart(t *testing.T) {
	gx := GdaxWebsocket{}
	assert := assert.New(t)
	acc := &testutil.Accumulator{}
	assert.Error(gx.Start(acc), "invalid config")

	gx = validTestGdaxWebsocket
	gx.Channels = gx.Channels[0:1]
	gx.validateConfig()
	require.NoError(t, gx.validateConfig(), "valid config")
	testError := errors.New("test dial: always error")
	dial = func(_ string) (conn, error) {
		return nil, testError
	}
	assert.Error(gx.Start(acc), "dial failure")

	wsConn := &mocks.Conn{}
	dial = func(_ string) (conn, error) {
		return wsConn, nil
	}
	err := errors.New("WriteJSON failed")
	wsConn.On("WriteJSON", mock.AnythingOfType("subscribeRequest")).
		Return(err).Once().
		On("Close").Return(nil).Once()
	assert.EqualError(gx.Start(acc), err.Error(), "WriteJSON failure")
	wsConn.AssertExpectations(t)

	wsConn = &mocks.Conn{}
	err = errors.New("ReadJSON failed")
	wsConn.On("WriteJSON", mock.AnythingOfType("subscribeRequest")).
		Return(nil).Once().
		On("ReadJSON", mock.AnythingOfType("*gdaxWebsocket.subscribeResponse")).
		Return(err).Once().
		On("Close").Return(nil).Once()
	assert.EqualError(gx.Start(acc), err.Error(), "ReadJSON failure")
	wsConn.AssertExpectations(t)

	wsConn = &mocks.Conn{}
	wsConn.On("WriteJSON", mock.AnythingOfType("subscribeRequest")).
		Return(nil).Once().
		On("ReadJSON", mock.AnythingOfType("*gdaxWebsocket.subscribeResponse")).
		Return(nil).Once().
		On("Close").Return(nil).Once()
	assert.Error(gx.Start(acc), "invalid subscription response")
	gx.Stop()
	wsConn.AssertExpectations(t)

	var request subscribeRequest
	wsConn = &mocks.Conn{}
	wsConn.On("WriteJSON", mock.AnythingOfType("subscribeRequest")).
		Return(func(req interface{}) error {
			request = req.(subscribeRequest)
			return nil
		}).Once().
		On("ReadJSON", mock.AnythingOfType("*gdaxWebsocket.subscribeResponse")).
		Return(func(r interface{}) error {
			res := r.(*subscribeResponse)
			res.subscribeRequest = request
			for _, ch := range res.Channels {
				ch.Pairs = append(ch.Pairs, res.Pairs...)
			}
			res.Pairs = nil
			res.Type = "subscriptions"
			return nil
		}).Once().
		On("ReadJSON", mock.AnythingOfType("*gdax.Message")).
		Return(nil).Once().
		On("ReadJSON", mock.AnythingOfType("*gdax.Message")).
		Return(err).Once().
		On("Close").Return(nil).Once()
	assert.NoError(gx.Start(acc), "valid subscription response")
	time.Sleep(1 * time.Second)
	gx.Stop()
	wsConn.AssertExpectations(t)
}

func TestValidateSubscribeResponse(t *testing.T) {
	req := subscribeRequest{
		Type:  "subscribe",
		Pairs: []string{"ETH-USD"},
		Channels: []channelConfig{
			{Channel: "ticker", Pairs: []string{"ETH-BTC"}},
			{Channel: "level2"},
		},
	}

	res := subscribeResponse{
		subscribeRequest{
			Type: "subscriptions",
			Channels: []channelConfig{
				{Channel: "ticker", Pairs: []string{"ETH-USD", "ETH-BTC"}},
				{Channel: "level2", Pairs: []string{"ETH-USD"}},
			},
		},
	}
	assert := assert.New(t)
	assert.NoError(validateSubscribeResponse(req, res), "valid subscribeResponse")
	chs := res.Channels
	res.Channels = chs[1:1]
	assert.Error(validateSubscribeResponse(req, res), "missing channel")
	res.Channels = append([]channelConfig{},
		channelConfig{Channel: "user", Pairs: []string{"ETH-USD"}}, chs[0])
	assert.Error(validateSubscribeResponse(req, res), "mismatch channel")
	res.Channels = chs
	res.Type = "nope"
	assert.Error(validateSubscribeResponse(req, res), "bad type")
	res.Type = "subscriptions"
	res.Channels[0].Pairs = []string{}
	assert.Error(validateSubscribeResponse(req, res), "bad pairs")
}

func TestValidateChannelPairs(t *testing.T) {
	reqChannelPairs := []string{"ETH-USD", "BTC-USD"}
	reqGlobalPairs := []string{"ETH-BTC"}
	resChannelPairs := []string{"ETH-BTC", "ETH-USD", "BTC-USD"}
	assert := assert.New(t)
	assert.NoError(validateChannelPairs(reqChannelPairs, reqGlobalPairs,
		resChannelPairs), "has global and channel pairs")
	assert.Error(validateChannelPairs(reqChannelPairs, reqGlobalPairs,
		append(resChannelPairs, "BTC-GBP")), "extra pair")
	assert.Error(validateChannelPairs(reqChannelPairs, reqGlobalPairs,
		append(resChannelPairs[0:2], "BTC-GBP")), "mismatched pair")
}

func TestWsDial(t *testing.T) {
	url := "example.com"
	c, err := wsDial(url)
	assert.Error(t, err, url)
	assert.Nil(t, c, url)

	url = "wss://ws-feed.gdax.com"
	c, err = wsDial(url)
	assert.NoError(t, err, url)
	assert.NotNil(t, c, url)
}

func init() {
	// Disable logging during tests
	lPrintln = func(_ ...interface{}) {}
}
