package normalizer

import (
	"context"
	"fmt"
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

	// --- Create receiver ---
	receiver, err := ristgo.ReceiverCreate(ctx, &ristgo.ReceiverConfig{
		RistProfile:             libristwrapper.RistProfileMain,
		LoggingCallbackFunction: createLogCB(identifier + "-rx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create RIST receiver: %w", err)
	}

	// --- Create sender ---
	sender, err := ristgo.CreateSender(ctx, &ristgo.SenderConfig{
		RistProfile:             libristwrapper.RistProfileMain,
		LoggingCallbackFunction: createLogCB(identifier + "-tx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		receiver.Destroy()
		return nil, fmt.Errorf("failed to create RIST sender: %w", err)
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

	logger.Info().Msg("Connected RIST sender→receiver loopback at 127.0.0.1:5000")

	return &Normalizer{
		sender:   sender,
		receiver: receiver,
	}, nil
}

func (n *Normalizer) Write(data []byte) error {
	if n.sender == nil {
		return fmt.Errorf("sender not initialized")
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
	return func(level libristwrapper.RistLogLevel, msg string) {
		msg = strings.TrimSuffix(msg, "\n")
		switch level {
		case libristwrapper.LogLevelError:
			logger.Error().Msg(msg)
		case libristwrapper.LogLevelWarn:
			logger.Warn().Msg(msg)
		case libristwrapper.LogLevelNotice, libristwrapper.LogLevelInfo:
			logger.Info().Msg(msg)
		case libristwrapper.LogLevelDebug:
			logger.Debug().Msg(msg)
		}
	}
}
