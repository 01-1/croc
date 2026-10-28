// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

//go:build !js

package tailcat

// magicsock only requests NAT-PMP, PCP, or UPnP port mappings when
// feature/portmapper has registered its constructor hook. tsnet and
// tailscaled link it, but Tailcat builds wgengine directly, so it must be
// linked here for mapped endpoints to reach advertiseEndpoints. condregister
// honors the ts_omit_portmapper build tag.
import _ "tailscale.com/feature/condregister/portmapper"
