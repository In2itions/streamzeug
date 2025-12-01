/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package normalizer

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"code.videolan.org/rist/ristgo"
	"code.videolan.org/rist/ristgo/libristwrapper"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/stats"
)

type Normalizer struct {
	sender     *ristgo.Sender
	receiver   ristgo.Receiver
	inMemory   bool
	dataCh     chan []byte
	cancelFunc context.CancelFunc
	OutChan    chan []byte
}

// New creates a RIST-based normalizer with automatic fallback to in-memory bridging.
func New(ctx context.Context, identifier string, s *stats.Stats) (*Normalizer, error) {
	logger := logging.Log.With().Str("module", "normalizer").Str("identifier", identifier).Logger()
	logger.Info().Msg("[STEP N1] Starting RIST normalizer initialization")

	// --- Create RIST receiver ---
	receiver, err := ristgo.ReceiverCreate(ctx, &ristgo.ReceiverConfig{
		RistProfile:             libristwrapper.RistProfileSimple,
		LoggingCallbackFunction: createLogCB(identifier + "-rx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		logger.Error().Err(err).Msg("[STEP N1-FAIL] Failed to create RIST receiver")
		return nil, fmt.Errorf("failed to create RIST receiver: %w", err)
	}
	logger.Info().Msg("[STEP N2] RIST receiver created successfully")
	time.Sleep(time.Second)

	// --- Create RIST sender ---
	sender, err := ristgo.CreateSender(ctx, &ristgo.SenderConfig{
		RistProfile:             libristwrapper.RistProfileSimple,
		LoggingCallbackFunction: createLogCB(identifier + "-tx"),
		StatsCallbackFunction:   createStatsCB(s),
		StatsInterval:           stats.StatsIntervalSeconds * 1000,
	})
	if err != nil {
		logger.Error().Err(err).Msg("[STEP N2-FAIL] Failed to create RIST sender")
		receiver.Destroy()
		return nil, fmt.Errorf("failed to create RIST sender: %w", err)
	}
	logger.Info().Msg("[STEP N3] RIST sender created successfully")
	time.Sleep(time.Second)

	norm := &Normalizer{
		sender:   sender,
		receiver: receiver,
		dataCh:   make(chan []byte, 4096),
		OutChan:  make(chan []byte, 4096),
	}

	// --- Attempt dynamic UDP peer mode ---
	logger.Info().Msg("[STEP N4] Attempting to create dynamic local UDP bridge between sender and receiver")

	success := false

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err == nil {
		localPort := listener.LocalAddr().(*net.UDPAddr).Port
		listener.Close()

		// Use localhost for better compatibility across IPv4/IPv6 systems
		senderURL, _ := url.Parse(fmt.Sprintf("rist://localhost:%d?profile=simple&cname=%s-tx", localPort, identifier))
		receiverURL, _ := url.Parse(fmt.Sprintf("rist://localhost:%d?profile=simple&cname=%s-rx", localPort, identifier))

		senderPeer, errSender := ristgo.ParseRistURL(senderURL)
		receiverPeer, errReceiver := ristgo.ParseRistURL(receiverURL)

		if errSender == nil && errReceiver == nil {
			logger.Debug().Msgf("[STEP N4a] Using dynamic RIST bridge: TX=%s RX=%s", senderURL.String(), receiverURL.String())

			if _, err := sender.AddPeer(senderPeer); err == nil {
				logger.Info().Msgf("[STEP N4b] Sender peer added on %s", senderURL.String())

				if _, err := receiver.AddPeer(receiverPeer); err == nil {
					logger.Info().Msgf("[STEP N4c] Receiver peer added — dynamic UDP bridge active (TX→RX on port %d)", localPort)
					success = true
				} else {
					logger.Warn().Err(err).Msg("[STEP N4c-FAIL] Failed to add receiver peer")
				}
			} else {
				logger.Warn().Err(err).Msg("[STEP N4b-FAIL] Failed to add sender peer")
			}
		} else {
			logger.Warn().Err(errSender).Err(errReceiver).Msg("[STEP N4-FAIL] Failed to parse RIST URLs")
		}
	} else {
		logger.Warn().Err(err).Msg("[STEP N4-FAIL] Failed to bind dynamic UDP port")
	}

	if success {
		time.Sleep(time.Second)
		return norm, nil
	}

	// --- In-memory fallback ---
	logger.Warn().Msg("[STEP N5] Activating in-memory fallback bridge (no UDP peer)")
	norm.inMemory = true
	readCtx, cancel := context.WithCancel(ctx)
	norm.cancelFunc = cancel

	go func() {
		logger.Info().Msg("[STEP N6] In-memory RIST bridge goroutine started (sender→receiver)")
		packetCount := 0
		for {
			select {
			case <-readCtx.Done():
				logger.Info().Msg("[STEP N6-END] In-memory RIST bridge stopped (context canceled)")
				return
			case pkt := <-norm.dataCh:
				packetCount++
				if packetCount%100 == 0 {
					logger.Debug().Int("packets", packetCount).Msg("[INMEM] Forwarding packet from dataCh to OutChan")
				}
				select {
				case norm.OutChan <- pkt:
				default:
					logger.Warn().Msg("[INMEM] OutChan full — dropping packet")
				}
			}
		}
	}()

	logger.Info().Msg("[STEP N7] Created in-memory RIST normalizer (sender→receiver fallback mode)")
	time.Sleep(time.Second)
	return norm, nil
}

// Write feeds packets into the RIST pipeline or in-memory channel.
func (n *Normalizer) Write(data []byte) error {
	logger := logging.Log.With().Str("module", "normalizer").Logger()

	if n.sender == nil {
		logger.Error().Msg("[WRITE-FAIL] Sender not initialized")
		return fmt.Errorf("sender not initialized")
	}

	if n.inMemory {
		select {
		case n.dataCh <- append([]byte(nil), data...):
			logger.Trace().Int("bytes", len(data)).Msg("[WRITE] Wrote packet into in-memory channel")
		default:
			logger.Warn().Msg("[WRITE] In-memory channel full — packet dropped")
		}
		return nil
	}

	_, err := n.sender.Write(data)
	if err != nil {
		logger.Error().Err(err).Msg("[WRITE-FAIL] Error writing data to sender")
	} else {
		logger.Trace().Int("bytes", len(data)).Msg("[WRITE] Packet sent through RIST sender")
	}
	return err
}

// Receiver exposes the RIST receiver handle for downstream consumers.
func (n *Normalizer) Receiver() ristgo.Receiver {
	return n.receiver
}

// Close gracefully stops all goroutines and RIST handles.
func (n *Normalizer) Close() {
	logger := logging.Log.With().Str("module", "normalizer").Logger()
	logger.Info().Msg("[CLOSE] Closing normalizer")

	if n.cancelFunc != nil {
		n.cancelFunc()
	}

	if n.sender != nil {
		logger.Info().Msg("[CLOSE] Closing RIST sender")
		n.sender.Close()
	}

	if n.receiver != nil {
		logger.Info().Msg("[CLOSE] Destroying RIST receiver")
		n.receiver.Destroy()
	}

	close(n.OutChan)
	logger.Info().Msg("[CLOSE] Normalizer closed successfully")
}

// --- helper functions (logging + stats) ---

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
