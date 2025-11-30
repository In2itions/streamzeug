package normalizer

import (
	"context"
	"fmt"
	"net/url"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"
)

type Normalizer struct {
	sender     *ristgo.Sender
	receiver   ristgo.Receiver
	inMemory   bool
	dataCh     chan []byte
	cancelFunc context.CancelFunc
	OutChan    chan []byte
}

// New creates a RIST-based normalizer with automatic fallback to in-memory bridging.
func New(ctx context.Context, identifier string, s *stats.Stats) (*Normalizer, error) {
	logger := logging.Log.With().Str("module", "normalizer").Str("identifier", identifier).Logger()

	receiver, err := ristgo.ReceiverCreate(ctx, &ristgo.ReceiverConfig{
		RistProfile:             libristwrapper.RistProfileMain,
		LoggingCallbackFunction: createLogCB(identifier + "-rx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create RIST receiver: %w", err)
	}

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

	norm := &Normalizer{
		sender:   sender,
		receiver: receiver,
		dataCh:   make(chan []byte, 4096),
		OutChan:  make(chan []byte, 4096),
	}

	ristURL, _ := url.Parse("rist://127.0.0.1:0")
	peerConfig, err := ristgo.ParseRistURL(ristURL)
	if err == nil {
		if _, err := sender.AddPeer(peerConfig); err == nil {
			logger.Info().Msg("Connected RIST sender→receiver via local UDP loopback (127.0.0.1:0)")
			return norm, nil
		}
		logger.Warn().Err(err).Msg("Failed to create UDP peer — switching to in-memory mode")
	} else {
		logger.Warn().Err(err).Msg("Failed to parse RIST URL — switching to in-memory mode")
	}

	norm.inMemory = true
	readCtx, cancel := context.WithCancel(ctx)
	norm.cancelFunc = cancel

	go func() {
		logger.Info().Msg("In-memory RIST bridge active (internal data channel)")
		for {
			select {
			case <-readCtx.Done():
				logger.Info().Msg("In-memory RIST bridge stopped")
				return
			case pkt := <-norm.dataCh:
				select {
				case norm.OutChan <- pkt:
				default:
				}
			}
		}
	}()

	logger.Info().Msg("Created in-memory RIST normalizer (sender→receiver fallback mode)")
	return norm, nil
}

func (n *Normalizer) Write(data []byte) error {
	if n.sender == nil {
		return fmt.Errorf("sender not initialized")
	}
	if n.inMemory {
		select {
		case n.dataCh <- append([]byte(nil), data...):
		default:
		}
		return nil
	}
	_, err := n.sender.Write(data)
	return err
}

func (n *Normalizer) Receiver() ristgo.Receiver { return n.receiver }

func (n *Normalizer) Close() {
	if n.cancelFunc != nil {
		n.cancelFunc()
	}
	if n.sender != nil {
		n.sender.Close()
	}
	if n.receiver != nil {
		n.receiver.Destroy()
	}
	close(n.OutChan)
}
