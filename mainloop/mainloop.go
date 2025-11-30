/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021-2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Gijs Peskens <gijs@peskens.net>
 * SPDX-FileContributor: Lucy (ChatGPT Assistant)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package mainloop

import (
	"context"
	"sync"
	"time"

	"code.videolan.org/rist/ristgo"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/output"
	"github.com/rs/zerolog"
)

type inputstatus struct {
	packetcount        int
	packetcountsince   int
	bytesSince         int
	discontinuitycount int
	lastPacketTime     time.Time
}

// Mainloop manages input data flow and fan-out to multiple outputs (UDP, SRT, etc.)
type Mainloop struct {
	ctx                context.Context
	flow               ristgo.ReceiverFlow
	logger             zerolog.Logger
	outputs            map[int]*out
	outPutAdd          chan output.Output
	outPutRemove       chan output.Output
	outRemoveIdx       chan int
	wg                 sync.WaitGroup
	statusLock         sync.Mutex
	primaryInputStatus inputstatus
	lastStatusCall     time.Time
}

// RemoveOutput by direct reference
func (m *Mainloop) RemoveOutput(o output.Output) {
	select {
	case <-m.ctx.Done():
		return
	default:
	}
	m.outPutRemove <- o
}

// removeOutputByID removes output by numeric index
func (m *Mainloop) removeOutputByID(idx int) {
	select {
	case <-m.ctx.Done():
		return
	default:
	}
	m.outRemoveIdx <- idx
}

// deleteOutput closes and removes an output from map
func (m *Mainloop) deleteOutput(idx int, o output.Output) {
	m.logger.Info().Msgf("deleting output: %s", o.String())
	close(m.outputs[idx].dataChan)
	delete(m.outputs, idx)
}

// AddOutput registers a new output
func (m *Mainloop) AddOutput(o output.Output) {
	if m == nil {
		logging.Log.Warn().Msg("Mainloop is nil — skipping AddOutput() to prevent crash")
		return
	}
	m.logger.Info().Msgf("adding output %s", o.String())
	select {
	case <-m.ctx.Done():
		return
	default:
	}
	m.outPutAdd <- o
}

// Wait waits for all goroutines to finish or timeout
func (m *Mainloop) Wait(timeout time.Duration) {
	c := make(chan bool)
	go func() {
		m.wg.Wait()
		c <- true
	}()
	select {
	case <-c:
	case <-time.After(timeout):
	}
}

// NewMainloop constructs a mainloop and starts its receive loop
func NewMainloop(ctx context.Context, flow ristgo.ReceiverFlow, identifier string) *Mainloop {
	m := &Mainloop{
		ctx:          ctx,
		flow:         flow,
		logger:       logging.Log.With().Str("identifier", identifier).Logger(),
		outputs:      make(map[int]*out),
		outPutAdd:    make(chan output.Output, 4),
		outPutRemove: make(chan output.Output, 4),
		outRemoveIdx: make(chan int, 16),
	}

	go receiveLoop(m)
	return m
}

// receiveLoop handles incoming RIST packets and fans them out to outputs.
func receiveLoop(m *Mainloop) {
	m.wg.Add(1)
	defer m.wg.Done()

	m.primaryInputStatus.lastPacketTime = time.Now()
	m.lastStatusCall = m.primaryInputStatus.lastPacketTime

	// 🛡 Safety guard: skip loop if no RIST flow (UDP/in-memory mode)
	if m.flow == nil {
		m.logger.Warn().Msg("Mainloop started without RIST flow (UDP/in-memory mode) — skipping receiveLoop")
		return
	}

	m.logger.Info().Msg("receiver mainloop started")
	outputidx := 0
	expectedSeq := uint16(0)
	lastDiscontinuityMsg := time.Time{}
	discontinuitiesSinceLastMsg := 0

main:
	for {
		select {
		case <-m.ctx.Done():
			break main

		case rb, ok := <-m.flow.DataChannel():
			if !ok {
				break main
			}

			discontinuity := rb.Discontinuity || rb.SeqNo != uint32(expectedSeq)
			if discontinuity {
				m.primaryInputStatus.discontinuitycount++
				discontinuitiesSinceLastMsg++
			}

			// Log discontinuities every 5s max
			if discontinuitiesSinceLastMsg > 0 && time.Since(lastDiscontinuityMsg) >= 5*time.Second {
				m.logger.Error().Int("count", discontinuitiesSinceLastMsg).Msg("discontinuity!")
				lastDiscontinuityMsg = time.Now()
				discontinuitiesSinceLastMsg = 0
			}

			expectedSeq = uint16(rb.SeqNo) + 1

			// Update stats
			m.statusLock.Lock()
			m.primaryInputStatus.packetcount++
			m.primaryInputStatus.packetcountsince++
			m.primaryInputStatus.bytesSince += len(rb.Data)
			m.primaryInputStatus.lastPacketTime = time.Now()
			m.statusLock.Unlock()

			m.writeOutputs(rb)

		case output := <-m.outPutAdd:
			m.statusLock.Lock()
			m.addOutput(output, outputidx)
			outputidx++
			m.statusLock.Unlock()

		case idx := <-m.outRemoveIdx:
			m.statusLock.Lock()
			if o, ok := m.outputs[idx]; ok {
				m.deleteOutput(idx, o.w)
			} else {
				m.logger.Error().Msgf("couldn't delete output at index: %d (not found)", idx)
			}
			m.statusLock.Unlock()

		case output := <-m.outPutRemove:
			found := false
			m.statusLock.Lock()
			for idx, o := range m.outputs {
				if o.w == output {
					found = true
					delete(m.outputs, idx)
					break
				}
			}
			m.statusLock.Unlock()
			if !found {
				m.logger.Error().Msgf("couldn't delete output: %s (not found)", output.String())
			}
		}
	}

	close(m.outPutAdd)
	close(m.outPutRemove)
	close(m.outRemoveIdx)
	m.logger.Info().Msg("mainloop terminated")
}
