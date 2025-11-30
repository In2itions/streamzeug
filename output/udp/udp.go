/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021-2025 ODMedia B.V.
 * SPDX-FileContributor: Author: Lucy (ChatGPT Assistant, based on original by Gijs Peskens)
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package udp

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"code.videolan.org/rist/ristgo/libristwrapper"
	"github.com/In2itions/streamzeug/logging"
	"github.com/In2itions/streamzeug/mainloop"
	"github.com/In2itions/streamzeug/output"
	"github.com/In2itions/streamzeug/vectorio"
	"golang.org/x/sys/unix"
)

const (
	tsPacketSize  = 188
	tsUdpPayload  = tsPacketSize * 7 // 1316 bytes per UDP frame
	reconnectWait = 50 * time.Millisecond
)

type socketOptFunc func(sc syscall.RawConn) error

type udpoutput struct {
	c          *net.UDPConn
	m          *mainloop.Mainloop
	ctx        context.Context
	cancel     context.CancelFunc
	float      bool
	identifier string
	source     *net.UDPAddr
	target     *net.UDPAddr
	name       string
	isRtp      bool
	rtpSeq     uint16
	rtpSSRC    uint32
	rtpHeader  []byte
	sc         syscall.RawConn
	ss         []socketOptFunc
	buf        []byte
}

// ---------------------------------------------------------
// Core Write Functions
// ---------------------------------------------------------

// writeAligned ensures TS-packet-aligned UDP output using a local buffer.
func (u *udpoutput) writeAligned(data []byte) (int, error) {
	u.buf = append(u.buf, data...)
	written := 0

	for len(u.buf) >= tsUdpPayload {
		n, err := u.c.Write(u.buf[:tsUdpPayload])
		if err != nil {
			return written, err
		}
		written += n
		u.buf = u.buf[tsUdpPayload:]
	}

	return written, nil
}

// writeRTP wraps MPEG-TS in RTP framing and sends it efficiently via writev().
func (u *udpoutput) writeRTP(block *libristwrapper.RistDataBlock) (int, error) {
	rtptime := (block.TimeStamp * 90000) >> 32
	h := u.rtpHeader

	h[0], h[1] = 0x80, 0x21&0x7f
	h[2], h[3] = byte(u.rtpSeq>>8), byte(u.rtpSeq)
	u.rtpSeq++
	h[4], h[5], h[6], h[7] = byte(rtptime>>24), byte(rtptime>>16), byte(rtptime>>8), byte(rtptime)
	h[8], h[9], h[10], h[11] = byte(u.rtpSSRC>>24), byte(u.rtpSSRC>>16), byte(u.rtpSSRC>>8), byte(u.rtpSSRC)

	bufs := [][]byte{h, block.Data}
	return vectorio.WritevSC(u.sc, bufs)
}

// ---------------------------------------------------------
// Interface Methods
// ---------------------------------------------------------

func (u *udpoutput) String() string { return u.name }

func (u *udpoutput) Count() int { return 1 }

// Write sends a RIST block to the UDP or RTP socket.
func (u *udpoutput) Write(block *libristwrapper.RistDataBlock) (int, error) {
	var (
		n   int
		err error
	)

	if u.isRtp {
		n, err = u.writeRTP(block)
	} else {
		n, err = u.writeAligned(block.Data)
	}

	if err == nil {
		return n, nil
	}

	// Handle recoverable network errors gracefully
	if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ECONNREFUSED) {
		return n, nil
	}

	if u.float {
		logging.Log.Info().Str("identifier", u.identifier).Msgf("floating UDP output: %s entered inactive state", u.name)
		go u.connectloop()
	}

	return n, err
}

// Close cleans up sockets and cancels the context.
func (u *udpoutput) Close() error {
	u.cancel()
	if u.c != nil {
		return u.c.Close()
	}
	return nil
}

// ---------------------------------------------------------
// Connection Handling
// ---------------------------------------------------------

// connect attempts to open a UDP socket and apply configured socket options.
func (u *udpoutput) connect() error {
	var err error
	u.c, err = net.DialUDP("udp", u.source, u.target)
	if err != nil {
		return err
	}

	u.sc, err = u.c.SyscallConn()
	if err != nil {
		return err
	}

	for _, s := range u.ss {
		if serr := s(u.sc); serr != nil {
			return serr
		}
	}

	return nil
}

// connectloop periodically retries reconnecting floating UDP outputs.
func (u *udpoutput) connectloop() {
	for {
		select {
		case <-u.ctx.Done():
			return
		default:
		}

		if err := u.connect(); err != nil {
			time.Sleep(reconnectWait)
			continue
		}

		logging.Log.Info().
			Str("identifier", u.identifier).
			Msgf("floating UDP output: %s entered active state", u.name)

		if u.m != nil {
			u.m.AddOutput(u)
		} else {
			logging.Log.Warn().
				Str("identifier", u.identifier).
				Msg("Mainloop is nil during connectloop — skipping AddOutput()")
		}
		return
	}
}

// ---------------------------------------------------------
// Parser Entry Point
// ---------------------------------------------------------

// ParseUdpOutput configures a new UDP or RTP output, including multicast TTL and reconnect handling.
func ParseUdpOutput(ctx context.Context, u *url.URL, identifier string, m *mainloop.Mainloop) (output.Output, error) {
	logger := logging.Log.With().Str("identifier", identifier).Logger()
	logger.Info().Msgf("Setting up UDP output: %s", u.String())

	out := udpoutput{
		name:       u.String(),
		identifier: identifier,
		float:      false,
		ss:         make([]socketOptFunc, 0),
		m:          m,
	}

	out.ctx, out.cancel = context.WithCancel(ctx)

	// Parse URL query options
	mcastIface := u.Query().Get("iface")
	if u.Query().Get("float") != "" {
		out.float = true
	}

	// RTP support
	if u.Scheme == "rtp" {
		out.isRtp = true
		out.rtpSSRC = rand.Uint32()
		out.rtpHeader = make([]byte, 12)
	}

	// TTL handling
	ttl := 255
	if ttlVal := u.Query().Get("ttl"); ttlVal != "" {
		if val, err := strconv.Atoi(ttlVal); err == nil {
			ttl = val
		} else {
			return nil, err
		}
	}

	// Resolve source interface if specified
	var sourceIP *net.UDPAddr
	if mcastIface != "" {
		if iface, err := net.InterfaceByName(mcastIface); err == nil {
			addrs, err := iface.Addrs()
			if err != nil {
				return nil, err
			}
			if len(addrs) > 0 {
				if ipnet, ok := addrs[0].(*net.IPNet); ok {
					sourceIP, err = net.ResolveUDPAddr("udp", ipnet.IP.String()+":0")
					if err != nil {
						return nil, err
					}
				}
			}
		} else if strings.Contains(mcastIface, ":") {
			sourceIP, err = net.ResolveUDPAddr("udp", mcastIface)
			if err != nil {
				return nil, err
			}
		} else {
			sourceIP, err = net.ResolveUDPAddr("udp", mcastIface+":0")
			if err != nil {
				return nil, err
			}
		}
	}

	// Resolve destination
	target, err := net.ResolveUDPAddr("udp", u.Host)
	if err != nil {
		return nil, err
	}
	out.source = sourceIP
	out.target = target

	// Multicast TTL
	if target.IP.IsMulticast() {
		ttlFunc := func(sc syscall.RawConn) error {
			var sockErr error
			err := sc.Control(func(fd uintptr) {
				sockErr = syscall.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_MULTICAST_TTL, ttl)
			})
			if err == nil {
				err = sockErr
			}
			return err
		}
		out.ss = append(out.ss, ttlFunc)
	}

	// Establish connection
	err = out.connect()
	if err != nil {
		if out.float && (errors.Is(err, unix.EADDRNOTAVAIL) || errors.Is(err, unix.ENETUNREACH)) {
			go out.connectloop()
			return &out, nil
		}
		return nil, err
	}

	// Safely register output
	if out.m != nil {
		out.m.AddOutput(&out)
	} else {
		logger.Warn().Msg("Mainloop is nil — skipping AddOutput() to prevent crash")
	}

	return &out, nil
}
