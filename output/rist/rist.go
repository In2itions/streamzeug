/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021 ODMedia B.V. All right reserved.
 * SPDX-FileContributor: Author: Gijs Peskens <gijs@peskens.net>
 * SPDX-FileContributor: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package rist

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/mainloop"
	"github.com/In2itions/streamzeug/output"
	"github.com/In2itions/streamzeug/stats"
)

// ristoutput implements the Output interface for RIST sending.
type ristoutput struct {
	sender    *ristgo.Sender
	clientURL string
}

// ParseRistOutput sets up a RIST sender that forwards packets from the mainloop.
func ParseRistOutput(ctx context.Context, u *url.URL, identifier, outputIdentifier string, m *mainloop.Mainloop, s *stats.Stats, wait *sync.WaitGroup) (output.Output, error) {
	logger := logging.Log.With().
		Str("module", "rist-output").
		Str("identifier", identifier).
		Str("output", outputIdentifier).
		Logger()

	logger.Info().Msgf("Setting up RIST output: %s", u.String())

	// --- Create sender ---
	sender, err := ristgo.CreateSender(ctx, &ristgo.SenderConfig{
		RistProfile:             libristwrapper.RistProfileMain,
		LoggingCallbackFunction: createLogCB(outputIdentifier),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create RIST sender")
		return nil, err
	}

	// --- Parse peer URL and add peer ---
	peerConfig, err := ristgo.ParseRistURL(u)
	if err != nil {
		sender.Close()
		logger.Error().Err(err).Msg("Failed to parse RIST output URL")
		return nil, err
	}

	if _, err := sender.AddPeer(peerConfig); err != nil {
		sender.Close()
		logger.Error().Err(err).Msg("Failed to add RIST peer")
		return nil, err
	}

	logger.Info().Msg("RIST output successfully configured")

	// Create output object
	out := &ristoutput{
		sender:    sender,
		clientURL: u.String(),
	}

	// Connect mainloop output
	wait.Add(1)
	go func() {
		defer wait.Done()
		m.RunOutput(out)
	}()

	return out, nil
}

func (r *ristoutput) Close() error {
	if r.sender != nil {
		r.sender.Close()
	}
	return nil
}

func (r *ristoutput) Count() int {
	return 1
}

func (r *ristoutput) String() string {
	return r.clientURL
}

// Write sends RIST data blocks via the sender.
func (r *ristoutput) Write(block *ristgo.RistDataBlock) (n int, e error) {
	if r.sender == nil || block == nil {
		return 0, nil
	}
	return r.sender.Write(block.Data)
}

// --- Local helpers for logging & stats ---
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
	logger := logging.Log.With().Str("module", "rist-output").Str("identifier", identifier).Logger()
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
