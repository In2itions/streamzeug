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

// Normalizer wraps arbitrary transport input into a RIST sender
// and internally connects it to a RIST receiver on loopback,
// so downstream pipeline logic stays compatible.
type Normalizer struct {
	sender   *ristgo.Sender
	receiver ristgo.Receiver
}

// New creates a Normalizer with paired RIST sender/receiver on loopback.
func New(ctx context.Context, identifier string, s *stats.Stats) (*Normalizer, error) {
	logger := logging.Log.With().Str("module", "normalizer").Str("identifier", identifier).Logger()

	// --- Create receiver ---
	logSettings := libristwrapper.CreateRistLoggingSettingsWithCB(createLogCB(identifier + "-rx"))
	receiverCtx, err := libristwrapper.ReceiverCreate(libristwrapper.RistProfileMain, logSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create RIST receiver: %w", err)
	}
	receiver := ristgo.Receiver{Ctx: receiverCtx}

	logger.Info().Msg("Created internal RIST receiver (loopback mode)")

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

	// --- Connect sender to receiver locally ---
	ristURL, _ := url.Parse("rist://127.0.0.1:5000")
	peerCfg, err := ristgo.ParseRistURL(ristURL)
	if err != nil {
		sender.Close()
		receiver.Destroy()
		return nil, err
	}

	if err := sender.AddPeer(peerCfg); err != nil {
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

// Write pushes a packet into the RIST sender.
func (n *Normalizer) Write(data []byte) error {
	if n.sender == nil {
		return fmt.Errorf("sender not initialized")
	}
	_, err := n.sender.Write(data)
	return err
}

// Receiver exposes the RIST receiver (for mainloop consumption).
func (n *Normalizer) Receiver() ristgo.Receiver {
	return n.receiver
}

// Close shuts down sender and receiver.
func (n *Normalizer) Close() {
	if n.sender != nil {
		n.sender.Close()
	}
	n.receiver.Destroy()
}

// --- Helpers for logging and stats (unchanged) ---

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
		case libristwrapper.LogLevelNotice:
			logger.Info().Msg(logmessage)
		case libristwrapper.LogLevelInfo:
			logger.Info().Msg(logmessage)
		case libristwrapper.LogLevelDebug:
			logger.Debug().Msg(logmessage)
		}
	}
}
