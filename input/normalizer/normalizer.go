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

	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
)

type Normalizer struct {
	sender     *ristgo.Sender
	receiver   ristgo.Receiver
	inMemory   bool
	dataCh     chan []byte
	cancelFunc context.CancelFunc
}

// New creates a RIST-based normalizer with automatic fallback to in-memory bridging.
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

	norm := &Normalizer{
		sender:   sender,
		receiver: receiver,
		dataCh:   make(chan []byte, 2048),
	}

	// --- Attempt normal UDP peer mode ---
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

	// --- Fallback: in-memory mode ---
	norm.inMemory = true
	readCtx, cancel := context.WithCancel(ctx)
	norm.cancelFunc = cancel

	go func() {
		logger.Info().Msg("In-memory RIST bridge active (direct injection)")
		for {
			select {
			case <-readCtx.Done():
				logger.Info().Msg("In-memory RIST bridge stopped")
				return
			case pkt := <-norm.dataCh:
				// simulate feed to receiver
				_ = norm.receiver.Write(pkt)
			}
		}
	}()

	logger.Info().Msg("Created in-memory RIST normalizer (sender→receiver direct injection)")
	return norm, nil
}

// Write pushes UDP packets into the RIST pipeline (real or in-memory).
func (n *Normalizer) Write(data []byte) error {
	if n.sender == nil {
		return fmt.Errorf("sender not initialized")
	}

	if n.inMemory {
		select {
		case n.dataCh <- append([]byte(nil), data...):
		default:
			// drop if full
		}
		return nil
	}

	_, err := n.sender.Write(data)
	return err
}

// Receiver returns the underlying RIST receiver handle.
func (n *Normalizer) Receiver() ristgo.Receiver {
	return n.receiver
}

// Close gracefully tears down sender, receiver, and bridge goroutine.
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
	logger.Info().Msg("Normalizer closed successfully")
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
