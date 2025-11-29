package normalizer

import (
	"context"
	"net/url"
	"strings"

	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
)

type Normalizer struct {
	sender   *ristgo.Sender
	receiver ristgo.Receiver
}

func New(ctx context.Context, identifier string, s *stats.Stats) (*Normalizer, error) {
	logger := logging.Log.With().Str("module", "normalizer").Str("identifier", identifier).Logger()

	// --- Create receiver (same as RIST input) ---
	receiver, err := ristgo.ReceiverCreate(ctx, &ristgo.ReceiverConfig{
		RistProfile:             libristwrapper.RistProfileSimple,
		LoggingCallbackFunction: createLogCB(identifier + "-rx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		return nil, err
	}

	// --- Create sender ---
	sender, err := ristgo.CreateSender(ctx, &ristgo.SenderConfig{
		RistProfile:             libristwrapper.RistProfileSimple,
		LoggingCallbackFunction: createLogCB(identifier + "-tx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		receiver.Destroy()
		return nil, err
	}

	// --- Connect sender → receiver internally ---
	ristURL, _ := url.Parse("rist://127.0.0.1:0")
	peerConfig, err := ristgo.ParseRistURL(ristURL)
	if err != nil {
		sender.Close()
		receiver.Destroy()
		return nil, err
	}

	if _, err := sender.AddPeer(peerConfig); err != nil {
		sender.Close()
		receiver.Destroy()
		return nil, err
	}

	logger.Info().Msg("Connected RIST sender→receiver loopback")

	return &Normalizer{
		sender:   sender,
		receiver: receiver,
	}, nil
}

func (n *Normalizer) Write(data []byte) error {
	if n.sender == nil {
		return nil
	}
	_, err := n.sender.Write(data)
	return err
}

func (n *Normalizer) Receiver() ristgo.Receiver {
	return n.receiver
}

func (n *Normalizer) Close() {
	if n.sender != nil {
		n.sender.Close()
	}
	n.receiver.Destroy()
}

func createStatsCB(s *stats.Stats) libristwrapper.StatsCallbackFunc {
	return func(statsData *libristwrapper.StatsContainer) {
		if statsData.ReceiverFlowStats != nil {
			s.HandleStats("", "", nil, statsData.ReceiverFlowStats)
		} else if statsData.SenderStats != nil {
			s.HandleStats("", "", nil, statsData.SenderStats)
		}
	}
}

func createLogCB(identifier string) libristwrapper.LogCallbackFunc {
	logger := logging.Log.With().Str("module", "normalizer").Str("identifier", identifier).Logger()
	return func(loglevel libristwrapper.RistLogLevel, logmessage string) {
		logmessage = strings.TrimSuffix(logmessage, "\n")
		switch loglevel {
		case libristwrapper.LogLevelError:
			logger.Error().Msg(logmessage)
		case libristwrapper.LogLevelWarn:
			logger.Warn().Msg(logmessage)
		case libristwrapper.LogLevelNotice, libristwrapper.LogLevelInfo:
			logger.Info().Msg(logmessage)
		case libristwrapper.LogLevelDebug:
			logger.Debug().Msg(logmessage)
		}
	}
}
