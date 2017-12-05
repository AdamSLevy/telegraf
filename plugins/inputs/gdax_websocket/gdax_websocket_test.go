package gdaxWebsocket

import (
	"testing"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"

	"github.com/stretchr/testify/assert"
)

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
	gx := &GdaxWebsocket{}
	assert := assert.New(t)

	gx.FeedURL = ""
	gx.Channels = []*channelConfig{{Channel: "ticker", Pairs: []string{"ETH-USD"}}}
	assert.Error(gx.validateConfig(), "empty FeedURL should be invalid")

	gx.FeedURL = "wss://ws-feed.gdax.com"
	gx.Channels = nil
	assert.Error(gx.validateConfig(), "empty channels should be invalid")

	gx.Channels = []*channelConfig{{}}
	assert.Error(gx.validateConfig(), "empty channel in channels should be invalid")

	gx.Channels[0].Channel = "ticker"
	assert.Error(gx.validateConfig(), "empty pairs should be invalid")

	gx.Channels[0].Channel = "invalid channel"
	gx.Channels[0].Pairs = []string{"ETH-BTC"}
	assert.Error(gx.validateConfig(), "invalid channel name should be invalid")

	validTestChannels := []string{"ticker", "level2"}
	for _, channel := range validTestChannels {
		gx.Channels[0].Channel = channel
		assert.NoError(gx.validateConfig(),
			"channel '%s' should be valid", channel)
		assert.Equal(gx.numUsers, 0,
			"numUsers should be 0 with no 'user' channel configs")

		channelConfigCopy := *gx.Channels[0]
		gx.Channels = append(gx.Channels, &channelConfigCopy)
		assert.Errorf(gx.validateConfig(),
			"multiple '%s' channels should be invalid",
			channel)
		gx.Channels = gx.Channels[:len(gx.Channels)-1]

		gx.Channels[0].UserName = "John Smith"
		assert.Errorf(gx.validateConfig(),
			"non-empty UserName for channel '%s' should be invalid",
			channel)
		gx.Channels[0].UserName = ""

		gx.Channels[0].Credentials = map[string]string{"test": "test"}
		assert.Errorf(gx.validateConfig(),
			"non-empty Credentials for channel '%s' should be invalid",
			channel)
		gx.Channels[0].Credentials = nil
	}

	gx.Channels[0].Channel = "user"
	assert.Error(gx.validateConfig(),
		"empty UserName in 'user' channel should be invalid")

	gx.Channels[0].UserName = "John Smith"
	assert.Error(gx.validateConfig(),
		"empty Credentials in 'user' channel should be invalid")

	gx.Channels[0].Credentials = map[string]string{"blah": "key"}
	assert.Error(gx.validateConfig(),
		"bad config key in Credentials in 'user' channel should be invalid")

	keys := []string{"key", "secret", "password"}
	gx.Channels[0].Credentials = make(map[string]string)
	for _, key := range keys {
		gx.Channels[0].Credentials[key] = key
	}
	for _, key := range keys {
		delete(gx.Channels[0].Credentials, key)
		assert.Errorf(gx.validateConfig(),
			"missing '%s' in Credentials in 'user' channel should be invalid",
			key)
		gx.Channels[0].Credentials[key] = ""
		assert.Errorf(gx.validateConfig(),
			"empty '%s' in Credentials in 'user' channel should be invalid",
			key)
		gx.Channels[0].Credentials[key] = key
	}

	assert.NoError(gx.validateConfig(),
		"'user' channel with UserName and Credentials should be valid")
	assert.Equal(gx.numUsers, 1,
		"numUsers should be 1 with one valid 'user' channel config")

	channelConfigCopy := *gx.Channels[0]
	gx.Channels = append(gx.Channels, &channelConfigCopy)
	assert.Error(gx.validateConfig(),
		"multiple 'user' channels with the same user_name should be invalid")

	gx.Channels[1].UserName = "Jane"
	assert.NoError(gx.validateConfig(),
		"multiple 'user' channels with unique user_names should be valid")
	assert.Equal(gx.numUsers, 2,
		"numUsers should be 2 with two valid 'user' channel configs")
	gx.Channels = gx.Channels[:len(gx.Channels)-1]
}

func TestStart(t *testing.T) {
	//gx := &GdaxWebsocket{}
	//assert := assert.New(t)
}
