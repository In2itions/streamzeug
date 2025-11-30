/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package normalizer

import (
	"context"
	"fmt"
	"net/url"
	"strings"

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

	// --- Create RIST receiver ---
	receiver, err := ristgo.ReceiverCreate(ctx, &ristgo.ReceiverConfig{
		RistProfile:             libristwrapper.RistProfileMain,
		LoggingCallbackFunction: createLogCB(identifier + "-rx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create RIST receiver: %w", err)
	}

	// --- Create RIST sender ---
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

	// --- Force a proper UDP peer for local bridging ---
	ristURL, _ := url.Parse("rist://@127.0.0.1:9000") // explicit port to force UDP bind
	peerConfig, err := ristgo.ParseRistURL(ristURL)
	if err == nil {
		if _, err := sender.AddPeer(peerConfig); err == nil {
			// Now the sender listens on UDP:9000 and receiver will connect to it
			if _, err := receiver.AddPeer(peerConfig); err == nil {
				logger.Info().Msg("RIST normalizer bridged via UDP loopback (127.0.0.1:9000)")
				return norm, nil
			}
			logger.Warn().Err(err).Msg("Failed to connect receiver peer — fallback to memory")
		} else {
			logger.Warn().Err(err).Msg("Failed to create sender peer — fallback to memory")
		}
	} else {
		logger.Warn().Err(err).Msg("Failed to parse RIST URL — fallback to memory")
	}

	// --- In-memory fallback ---
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
					// drop when channel full
				}
			}
		}
	}()

	logger.Info().Msg("Created in-memory RIST normalizer (sender→receiver fallback mode)")
	return norm, nil
}

// Write feeds packets into the RIST pipeline or in-memory channel.
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

// Receiver exposes the RIST receiver handle for downstream consumers.
func (n *Normalizer) Receiver() ristgo.Receiver {
	return n.receiver
}

// Close gracefully stops all goroutines and RIST handles.
func (n *Normalizer) Close() {
	logger := logging.Log.With().Str("module", "normalizer").Logger()

	if n.cancelFunc != nil {
		n.cancelFunc()
	}

	if n.sender != nil {
		logger.Info().Msg("Closing RIST sender")
		n.sender.Close()
	}

	if n.receiver != nil {
		logger.Info().Msg("Destroying RIST receiver")
		n.receiver.Destroy()
	}

	close(n.OutChan)
	logger.Info().Msg("Normalizer closed successfully")
}

// --- helper functions (logging + stats) ---

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
