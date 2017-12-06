package gdaxWebsocket

import (
	"testing"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"

	"github.com/stretchr/testify/assert"
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
		Secret:     "a531631f192bf4778a19fbfbfae237a5",
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

func TestServiceInput(t *testing.T) {
	gx := &GdaxWebsocket{}
	acc := &testutil.Accumulator{}
	assert := assert.New(t)

	assert.Implements((*telegraf.ServiceInput)(nil), gx,
		"should implement telegraf.ServiceInput")

	assert.NotEmpty(gx.SampleConfig(), "sample config should not be empty")

	assert.NotEmpty(gx.Description(), "description should not be empty")

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
	gx := validTestGdaxWebsocket
	require.NoError(t, gx.validateConfig(), "valid config")
	assert := assert.New(t)
	subs := gx.generateSubscribeRequests()
	assert.Len(subs, 2, "config with 2 users should return 2 subscribeRequests")
	for _, sub := range subs {
		assert.NotEmpty(sub.Key, "config with a user should have a non-empty Key")
		assert.NotEmpty(sub.Signature,
			"config with a user should have a non-empty Signature")
		assert.NotEmpty(sub.Passphrase,
			"config with a user should have a non-empty Passphrase")
		assert.NotEmpty(sub.Timestamp,
			"config with a user should have a non-empty Timestamp")
	}

	gx.Channels = gx.Channels[:len(gx.Channels)-1]
	require.NoError(t, gx.validateConfig(), "valid config")
	subs = gx.generateSubscribeRequests()
	assert.Len(subs, 1, "config with 1 user should return 1 subscribeRequest")
	assert.NotEmpty(subs[0].Key, "config with a user should have a non-empty Key")
	assert.NotEmpty(subs[0].Signature,
		"config with a user should have a non-empty Signature")
	assert.NotEmpty(subs[0].Passphrase,
		"config with a user should have a non-empty Passphrase")
	assert.NotEmpty(subs[0].Timestamp,
		"config with a user should have a non-empty Timestamp")
}

//func TestStart(t *testing.T) {
//	gx := &GdaxWebsocket{}
//	assert := assert.New(t)
//}
