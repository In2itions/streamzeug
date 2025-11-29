/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021 ODMedia B.V. All right reserved.
 * SPDX-FileContributor: Author: Gijs Peskens <gijs@peskens.net>
 * SPDX-FileContributor: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package flow

import (
	"fmt"
	"net/url"

	"github.com/In2itions/streamzeug/config"
	"github.com/In2itions/streamzeug/input/rist"
	"github.com/In2itions/streamzeug/input/udp"
	"github.com/In2itions/streamzeug/logging"
)

func (f *Flow) setupInput(c *config.Input) error {
	u, err := url.Parse(c.Url)
	if err != nil {
		return err
	}

	logger := logging.Log.With().
		Str("module", "flow-input").
		Str("identifier", f.config.Identifier).
		Str("url", c.Url).
		Logger()

	logger.Info().Msg("Configuring input")

	switch u.Scheme {
	case "rist":
		input, err := rist.SetupRistInput(u, f.config.Identifier, f.receiver)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to set up RIST input")
			return err
		}
		f.configuredInputs[c.Url] = input
		logger.Info().Msg("RIST input successfully configured")

	case "udp", "rtp":
		input, err := udp.SetupUdpInput(f.context, u, f.config.Identifier, f.statsConfig)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to set up UDP/RTP input")
			return fmt.Errorf("failed to setup udp/rtp input: %w", err)
		}
		f.configuredInputs[c.Url] = input
		logger.Info().Msg("UDP/RTP input successfully configured")

	default:
		err := fmt.Errorf("unsupported input type: %s", u.Scheme)
		logger.Error().Err(err).Msg("Invalid input type in configuration")
		return err
	}

	return nil
}
