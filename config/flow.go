/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021 ODMedia B.V. All right reserved.
 * SPDX-FileContributor: Author: Gijs Peskens <gijs@peskens.net>
 * SPDX-FileContributor: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"code.videolan.org/rist/ristgo/libristwrapper"
)

type Flow struct {
	Identifier      string                     `yaml:"identifier"`
	InputType       string                     `yaml:"type"` // RIST or UDP
	RistProfile     libristwrapper.RistProfile `yaml:"ristprofile"`
	Latency         int                        `yaml:"latency"`
	StreamID        int                        `yaml:"streamid"`
	Inputs          []Input                    `yaml:"inputs"`
	Outputs         []Output                   `yaml:"outputs"`
	StatsStdOut     bool                       `yaml:"statsstdout"`
	StatsFile       string                     `yaml:"statsfile"`
	MinimalBitrate  int                        `yaml:"minimalbitrate"`
	MaxPacketTimeMS int                        `yaml:"maxpackettime"`
}

func ValidateFlowConfig(c *Flow) error {
	if len(c.Inputs) < 1 {
		return errors.New("at least 1 input required")
	}

	if err := checkDuplicates(c.Inputs); err != nil {
		return err
	}

	for _, i := range c.Inputs {
		if err := validateInputConfig(&i); err != nil {
			return fmt.Errorf("input validation failed: %w", err)
		}
	}

	if err := checkDuplicates(c.Outputs); err != nil {
		return err
	}

	for _, o := range c.Outputs {
		if err := validateOutputConfig(&o); err != nil {
			return fmt.Errorf("output validation failed: %w", err)
		}
	}

	if c.Identifier == "" {
		return errors.New("flow must have non-empty Identifier")
	}

	// Normalize input type
	c.InputType = strings.ToUpper(c.InputType)
	if c.InputType == "" {
		c.InputType = "RIST" // default for backward compatibility
	}

	// --- Flow type validation ---
	switch c.InputType {
	case "RIST":
		if c.RistProfile > libristwrapper.RistProfileMain {
			return errors.New("invalid RistProfile")
		}
		if c.StreamID > math.MaxUint16 {
			return fmt.Errorf("StreamID: %d must be smaller than: %d", c.StreamID, math.MaxUint16)
		}

	case "UDP":
		// For UDP, no RIST-specific checks
		if c.RistProfile != 0 {
			fmt.Printf("warning: RistProfile ignored for UDP flow\n")
		}
		if c.StreamID != 0 {
			fmt.Printf("warning: StreamID ignored for UDP flow\n")
		}

	default:
		return fmt.Errorf("unsupported flow type: %s (must be RIST or UDP)", c.InputType)
	}

	if c.StatsFile != "" {
		if _, err := os.Stat(c.StatsFile); err != nil {
			return fmt.Errorf("statsfile: %s error: %w", c.StatsFile, err)
		}
	}

	if c.MaxPacketTimeMS > 0 && c.MinimalBitrate == 0 || c.MinimalBitrate > 0 && c.MaxPacketTimeMS == 0 {
		return errors.New("when using MaxPacketTime or MinimalBitrate both have to be set higher than 0")
	}

	return nil
}
