/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021–2025 ODMedia B.V.
 * SPDX-FileContributor: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package rist

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/In2itions/streamzeug/input"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
)

// ristinput represents a RIST input connection.
type ristinput struct {
	r ristgo.Receiver
	p int
}

// SetupReceiver creates a RIST receiver with logging and stats.
func SetupReceiver(ctx context.Context, identifier string, profile libristwrapper.RistProfile, recoverysize int, s *stats.Stats) (ristgo.Receiver, error) {
	logger := logging.Log.With().Str("module", "rist-input").Str("identifier", identifier).Logger()

	logger.Info().Msg("Starting RIST receiver")

	r, err := ristgo.ReceiverCreate(ctx, &ristgo.ReceiverConfig{
		RistProfile:             profile,
		LoggingCallbackFunction: createLogCB(identifier),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
		RecoveryBufferSize:      recoverysize,
	})
	if err != nil {
		return r, fmt.Errorf("failed to create receiver: %w", err)
	}

	return r, nil
}

// SetupRistInput registers an input peer to an existing RIST receiver.
func SetupRistInput(u *url.URL, identifier string, r ristgo.Receiver) (input.Input, error) {
	logger := logging.Log.With().Str("module", "rist-input").Str("identifier", identifier).Logger()
	logger.Info().Msgf("Setting up RIST input: %s", u.String())

	peerConfig, err := ristgo.ParseRistURL(u)
	if err != nil {
		return nil, fmt.Errorf("failed to parse RIST URL: %w", err)
	}

	id, err := r.AddPeer(peerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to add peer: %w", err)
	}

	return &ristinput{
		r: r,
		p: id,
	}, nil
}

// Close removes the RIST peer from the receiver.
func (i *ristinput) Close() {
	if err := i.r.RemovePeer(i.p); err != nil {
		logging.Log.Error().Err(err).Msg("error removing RIST peer")
	}
}

// --- Helper callbacks ---

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
	logger := logging.Log.With().Str("module", "rist-input").Str("identifier", identifier).Logger()
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
