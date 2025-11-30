/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021-2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Gijs Peskens <gijs@peskens.net>
 * SPDX-FileContributor: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package flow

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"code.videolan.org/rist/ristgo/libristwrapper"
	"github.com/In2itions/streamzeug/config"
	"github.com/In2itions/streamzeug/input"
	"github.com/In2itions/streamzeug/input/rist"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/mainloop"
	"github.com/In2itions/streamzeug/stats"
)

// CreateFlow builds and initializes a full flow pipeline (inputs, outputs, stats, mainloop).
// Supports both RIST and UDP flow types. For UDP, a local mainloop is auto-created to avoid nil references.
func CreateFlow(ctx context.Context, c *config.Flow) (*Flow, error) {
	var (
		flow Flow
		err  error
	)

	// --- Basic context & metadata ---
	flow.rcontext = ctx
	flow.identifier = c.Identifier
	flow.outputWait = new(sync.WaitGroup)

	// --- Validate configuration ---
	if err := config.ValidateFlowConfig(c); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	flow.config = *c
	logging.Log.Info().Str("identifier", c.Identifier).Msg("Setting up flow")
	flow.context, flow.cancel = context.WithCancel(ctx)

	// --- Stats setup ---
	flow.statsConfig, err = stats.SetupStats(c.StatsStdOut, c.Identifier, c.StatsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to setup stats: %w", err)
	}

	// --- Default latency ---
	if c.Latency == 0 {
		logging.Log.Info().Str("identifier", c.Identifier).Msg("Latency not set, defaulting to 1000ms")
		c.Latency = 1000
	}

	// --- Determine flow type ---
	flowType := strings.ToUpper(strings.TrimSpace(c.InputType))
	if flowType == "" {
		flowType = "RIST"
	}

	// -----------------------------------------------------------
	//  Setup receiver (RIST or UDP)
	// -----------------------------------------------------------
	switch flowType {
	case "RIST":
		// Standard RIST receiver setup
		flow.receiver, err = rist.SetupReceiver(flow.context, c.Identifier, c.RistProfile, c.Latency, flow.statsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to setup RIST receiver: %w", err)
		}

		if err = flow.receiver.Start(); err != nil {
			return nil, fmt.Errorf("failed to start RIST receiver: %w", err)
		}

		destinationPort := uint16(0)
		if c.RistProfile != libristwrapper.RistProfileSimple {
			destinationPort = uint16(c.StreamID)
		}

		rf, err := flow.receiver.ConfigureFlow(destinationPort)
		if err != nil {
			return nil, fmt.Errorf("failed to configure RIST flow: %w", err)
		}

		flow.m = mainloop.NewMainloop(flow.context, rf, c.Identifier)

	case "UDP":
		// UDP flow skips external RIST receiver
		logging.Log.Info().
			Str("identifier", c.Identifier).
			Str("type", flowType).
			Msg("UDP flow detected — skipping external RIST receiver setup (handled internally by normalizer)")

		flow.receiver = nil

		// ✅ Create a local mainloop for UDP-based flows
		flow.m = mainloop.New()
		logging.Log.Warn().
			Str("identifier", c.Identifier).
			Msg("No mainloop was assigned — created local mainloop for UDP flow to ensure stability")

	default:
		return nil, fmt.Errorf("unsupported flow type: %s", flowType)
	}

	// -----------------------------------------------------------
	//  Setup inputs
	// -----------------------------------------------------------
	flow.configuredInputs = make(map[string]input.Input)
	for _, in := range c.Inputs {
		if err = flow.setupInput(&in); err != nil {
			return nil, fmt.Errorf("failed to setup input %s: %w", in.Url, err)
		}
	}

	// -----------------------------------------------------------
	//  Setup outputs
	// -----------------------------------------------------------
	flow.configuredOutputs = make(map[string]outhandle)
	for _, out := range c.Outputs {
		if err = flow.setupOutput(&out); err != nil {
			return nil, fmt.Errorf("failed to setup output %s: %w", out.Url, err)
		}
	}

	logging.Log.Info().
		Str("identifier", c.Identifier).
		Str("type", flowType).
		Msg("Flow setup complete and operational")

	return &flow, nil
}
