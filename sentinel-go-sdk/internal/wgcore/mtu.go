package wgcore

// fitMTU lowers the node's MTU so the client tunnel fits its own uplink to host (e914dc7, UPSTREAM-GAPS
// #26): a tunnel MTU above uplink - overhead fragments or black-holes (TLS through WG/AWG hung on a
// 1460-byte uplink; AWG at 1420 moved ~3 Mbps). Never raises the node's MTU. The helpers (wgOverhead,
// overheader, uplinkMTU, fitMTUWith) are the node's (mtu_linux.go) and mtu_uplink_other.go elsewhere.
func fitMTU[E any](nodeMTU uint16, extensions E, host string) uint16 {
	overhead := wgOverhead
	if o, ok := any(extensions).(overheader); ok {
		overhead += o.TransportOverhead()
	}

	return fitMTUWith(nodeMTU, overhead, uplinkMTU(host))
}
