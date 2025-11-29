/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021 ODMedia B.V. All right reserved.
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

func CreateFlow(ctx context.Context, c *config.Flow) (*Flow, error) {
	var flow Flow
	var err error
	flow.rcontext = ctx
	flow.identifier = c.Identifier
	flow.outputWait = new(sync.WaitGroup)

	if err := config.ValidateFlowConfig(c); err != nil {
		return nil, fmt.Errorf("config validation failed %w", err)
	}
	flow.config = *c
	logging.Log.Info().Str("identifier", c.Identifier).Msg("setting up flow")
	flow.context, flow.cancel = context.WithCancel(ctx)

	flow.statsConfig, err = stats.SetupStats(c.StatsStdOut, c.Identifier, c.StatsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to setup stats %w", err)
	}

	if c.Latency == 0 {
		logging.Log.Info().Str("identifier", c.Identifier).Msg("setting latency to default of 1000ms")
		c.Latency = 1000
	}

	// Determine flow type (default RIST for backward compatibility)
	flowType := strings.ToUpper(c.InputType)
	if flowType == "" {
		flowType = "RIST"
	}

	// --- Setup Receiver (only for RIST) ---
	if flowType == "RIST" {
		flow.receiver, err = rist.SetupReceiver(flow.context, c.Identifier, c.RistProfile, c.Latency, flow.statsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to setup rist receiver %w", err)
		}
	} else {
		logging.Log.Info().Str("identifier", c.Identifier).
			Str("type", flowType).
			Msg("UDP flow detected — skipping external RIST receiver setup (handled by normalizer)")
		flow.receiver = nil
	}

	// --- Setup Inputs ---
	flow.configuredInputs = make(map[string]input.Input)
	for _, i := range c.Inputs {
		err = flow.setupInput(&i)
		if err != nil {
			return nil, fmt.Errorf("failed to setup input %s: %w", i.Url, err)
		}
	}

	// --- Start mainloop for RIST or UDP ---
	if flowType == "RIST" {
		destinationPort := uint16(0)
		if c.RistProfile != libristwrapper.RistProfileSimple {
			destinationPort = uint16(c.StreamID)
		}

		err = flow.receiver.Start()
		if err != nil {
			return nil, fmt.Errorf("failed to start rist receiver %w", err)
		}

		rf, err := flow.receiver.ConfigureFlow(destinationPort)
		if err != nil {
			return nil, fmt.Errorf("failed to configure rist flow %w", err)
		}

		m := mainloop.NewMainloop(flow.context, rf, c.Identifier)
		flow.m = m
	} else if flowType == "UDP" {
		logging.Log.Info().
			Str("identifier", c.Identifier).
			Msg("UDP flow setup complete — mainloop fed via internal RIST normalizer")
	}

	// --- Setup Outputs ---
	flow.configuredOutputs = make(map[string]outhandle)
	for _, o := range c.Outputs {
		err := flow.setupOutput(&o)
		if err != nil {
			return nil, fmt.Errorf("failed to setup output %s: %w", o.Url, err)
		}
	}

	return &flow, nil
}
