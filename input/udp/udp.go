/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package udp

import (
	"context"
	"net"
	"net/url"

	"github.com/In2itions/streamzeug/input"
	"github.com/In2itions/streamzeug/input/normalizer"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"
)

type udpinput struct {
	conn       *net.UDPConn
	cancel     context.CancelFunc
	normalizer *normalizer.Normalizer
}

// SetupUdpInput initializes the UDP/RTP listener and attaches it to the RIST normalizer.
func SetupUdpInput(ctx context.Context, u *url.URL, identifier string, s *stats.Stats) (input.Input, error) {
	logger := logging.Log.With().Str("module", "udp-input").Str("identifier", identifier).Logger()
	logger.Info().Msgf("Setting up UDP/RTP input: %s", u.String())

	addr, err := net.ResolveUDPAddr("udp", u.Host)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to resolve UDP address")
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to listen on UDP address")
		return nil, err
	}
	logger.Info().Msgf("Listening for UDP packets on %s", conn.LocalAddr().String())

	norm, err := normalizer.New(ctx, identifier, s)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create normalizer")
		conn.Close()
		return nil, err
	}
	logger.Info().Msg("Normalizer successfully initialized, internal RIST receiver connected to mainloop")

	readCtx, cancel := context.WithCancel(ctx)
	udpObj := &udpinput{
		conn:       conn,
		cancel:     cancel,
		normalizer: norm,
	}

	go udpObj.run(readCtx, identifier)
	logger.Info().Msg("UDP/RTP input setup complete and running")

	return udpObj, nil
}

func (u *udpinput) run(ctx context.Context, identifier string) {
	logger := logging.Log.With().Str("module", "udp-input").Str("identifier", identifier).Logger()
	buf := make([]byte, 1500)
	packetCount := 0

	logger.Info().Msg("UDP input read loop started")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("UDP input read loop exiting (context canceled)")
			return
		default:
			n, remoteAddr, err := u.conn.ReadFromUDP(buf)
			if err != nil {
				logger.Error().Err(err).Msg("Error reading UDP packet")
				continue
			}
			packetCount++
			if packetCount%1000 == 0 {
				logger.Debug().Int("packets", packetCount).Msg("Processed UDP packets")
			}

			if err := u.normalizer.Write(buf[:n]); err != nil {
				logger.Warn().Err(err).
					Int("bytes", n).
					Str("from", remoteAddr.String()).
					Msg("Failed to write packet to normalizer")
			} else {
				logger.Debug().
					Int("bytes", n).
					Str("from", remoteAddr.String()).
					Msg("UDP packet normalized and forwarded")
			}
		}
	}
}

func (u *udpinput) Close() {
	logger := logging.Log.With().Str("module", "udp-input").Logger()
	logger.Info().Msg("Closing UDP input")

	u.cancel()

	if u.conn != nil {
		logger.Info().Msgf("Closing UDP socket %s", u.conn.LocalAddr().String())
		u.conn.Close()
	}

	if u.normalizer != nil {
		logger.Info().Msg("Closing normalizer and RIST loopback")
		u.normalizer.Close()
	}

	logger.Info().Msg("UDP input closed successfully")
}
