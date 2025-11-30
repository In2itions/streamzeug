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
	"time"

	"code.videolan.org/rist/ristgo/libristwrapper"
	"github.com/In2itions/streamzeug/input"
	"github.com/In2itions/streamzeug/input/normalizer"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/output"
	"github.com/In2itions/streamzeug/stats"
)

type udpinput struct {
	conn       *net.UDPConn
	cancel     context.CancelFunc
	normalizer *normalizer.Normalizer
	outputs    []output.Output
}

// SetupUdpInput initializes UDP input and attaches it to the RIST normalizer.
func SetupUdpInput(ctx context.Context, u *url.URL, identifier string, s *stats.Stats) (input.Input, error) {
	logger := logging.Log.With().Str("module", "udp-input").Str("identifier", identifier).Logger()
	logger.Info().Msgf("[STEP 1] Starting UDP/RTP input setup for %s", u.String())

	addr, err := net.ResolveUDPAddr("udp", u.Host)
	if err != nil {
		logger.Error().Err(err).Msg("[STEP 1-FAIL] Failed to resolve UDP address")
		return nil, err
	}
	logger.Info().Msgf("[STEP 2] UDP address resolved: %s", addr.String())
	time.Sleep(time.Second)

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		logger.Error().Err(err).Msg("[STEP 2-FAIL] Failed to bind UDP socket")
		return nil, err
	}
	logger.Info().Msgf("[STEP 3] Listening for UDP packets on %s", conn.LocalAddr().String())
	time.Sleep(time.Second)

	norm, err := normalizer.New(ctx, identifier, s)
	if err != nil {
		logger.Error().Err(err).Msg("[STEP 3-FAIL] Failed to create normalizer")
		conn.Close()
		return nil, err
	}
	logger.Info().Msg("[STEP 4] Normalizer successfully initialized, internal RIST receiver connected")
	time.Sleep(time.Second)

	readCtx, cancel := context.WithCancel(ctx)
	udpObj := &udpinput{
		conn:       conn,
		cancel:     cancel,
		normalizer: norm,
		outputs:    []output.Output{},
	}

	logger.Info().Msg("[STEP 5] Launching normalizer→output forwarding goroutine")
	go func() {
		step := 0
		for pkt := range norm.OutChan {
			step++
			if step%100 == 0 {
				logger.Debug().Int("packets", step).Msg("[FORWARD] Received packet from normalizer OutChan")
			}
			block := &libristwrapper.RistDataBlock{
				Data:      pkt,
				TimeStamp: time.Now().UnixNano(), // int64 expected
			}
			for i, out := range udpObj.outputs {
				logger.Trace().Int("outputIndex", i).Msg("[FORWARD] Writing packet to output")
				out.Write(block)
			}
		}
		logger.Warn().Msg("[FORWARD] Normalizer OutChan closed — forwarding goroutine exiting")
	}()
	time.Sleep(time.Second)

	logger.Info().Msg("[STEP 6] Launching UDP read loop")
	go udpObj.run(readCtx, identifier)
	time.Sleep(time.Second)

	logger.Info().Msg("[STEP 7] UDP/RTP input setup complete and running")
	return udpObj, nil
}

func (u *udpinput) run(ctx context.Context, identifier string) {
	logger := logging.Log.With().Str("module", "udp-input").Str("identifier", identifier).Logger()
	buf := make([]byte, 1500)
	packetCount := 0

	logger.Info().Msg("[RUN] UDP input read loop started")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("[RUN] UDP input read loop exiting (context canceled)")
			return
		default:
			n, remoteAddr, err := u.conn.ReadFromUDP(buf)
			if err != nil {
				logger.Error().Err(err).Msg("[RUN-ERR] Error reading UDP packet")
				continue
			}
			packetCount++
			if packetCount%100 == 0 {
				logger.Info().
					Int("packets", packetCount).
					Str("from", remoteAddr.String()).
					Msg("[RUN] Received UDP packets successfully")
			}

			if err := u.normalizer.Write(buf[:n]); err != nil {
				logger.Warn().Err(err).
					Int("bytes", n).
					Str("from", remoteAddr.String()).
					Msg("[RUN-WARN] Failed to write packet to normalizer")
			} else if packetCount%100 == 0 {
				logger.Debug().Int("bytes", n).Msg("[RUN] Packet passed to normalizer successfully")
			}
		}
	}
}

func (u *udpinput) Close() {
	logger := logging.Log.With().Str("module", "udp-input").Logger()
	logger.Info().Msg("[CLOSE] Closing UDP input")

	u.cancel()

	if u.conn != nil {
		logger.Info().Msgf("[CLOSE] Closing UDP socket %s", u.conn.LocalAddr().String())
		u.conn.Close()
	}

	if u.normalizer != nil {
		logger.Info().Msg("[CLOSE] Closing normalizer and RIST bridge")
		u.normalizer.Close()
	}

	logger.Info().Msg("[CLOSE] UDP input closed successfully")
}
