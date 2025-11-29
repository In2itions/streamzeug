/*
 * SPDX-FileCopyrightText: Streamzeug Copyright © 2021 ODMedia B.V. All right reserved.
 * SPDX-FileContributor: Author: Gijs Peskens <gijs@peskens.net>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package output

import "code.videolan.org/rist/ristgo"

type Output interface {
	Close() error
	Write(block *ristgo.RistDataBlock) (n int, err error)
	String() string
	Count() int
}
