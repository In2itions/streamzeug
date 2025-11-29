/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package normalizer

import (
	"context"
	"strings"

	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
)

// Normalizer wraps arbitrary transport input into a RIST sender
// so that all downstream pipeline logic stays compatible.
type Normalizer struct {
	sender ristgo.Sender
}

// New creates a Normalizer with its own RIST sender instance.
func New(ctx context.Context, identifier string, s *stats.Stats) (*Normalizer, error) {
	ristsender, err := ristgo.SenderCreate(ctx, &ristgo.SenderConfig{
		RistProfile:             libristwrapper.ProfileMain,
		LoggingCallbackFunction: createLogCB(identifier),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		return nil, err
	}

	return &Normalizer{sender: ristsender}, nil
}

// Write pushes a packet into the RIST sender.
func (n *Normalizer) Write(data []byte) error {
	return n.sender.Write(data)
}

// Close shuts down the RIST sender.
func (n *Normalizer) Close() {
	if n.sender != nil {
		n.sender.Close()
	}
}

// --- Helpers for logging and stats (same pattern as RIST input) ---

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
