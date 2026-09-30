"""
Bitcoin P2P protocol implementation (minimal but correct for crawling).

Focus:
- Version handshake
- getaddr / addr (v1 + v2 support)
- ping/pong for liveness + basic latency
- Proper message framing + checksum (double SHA256)
- Network magic for mainnet (testnet easy to add later)

References (from Bitcoin protocol docs):
- https://en.bitcoin.it/wiki/Protocol_documentation
- https://developer.bitcoin.org/reference/p2p_networking.html
"""

from __future__ import annotations

import hashlib
import ipaddress
import socket
import struct
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Mainnet magic
MAGIC = b"\xf9\xbe\xb4\xd9"
MAGIC_TESTNET = b"\x0b\x11\x09\x07"

# Service flags (common ones)
NODE_NETWORK = 1 << 0          # Full node, can serve blocks
NODE_GETUTXO = 1 << 1
NODE_BLOOM = 1 << 2
NODE_WITNESS = 1 << 3
NODE_XTHIN = 1 << 4
NODE_COMPACT_FILTERS = 1 << 6
NODE_NETWORK_LIMITED = 1 << 10
NODE_P2P_V2 = 1 << 11          # BIP324 v2 encrypted transport

# Commands (12 bytes, null padded)
CMD_VERSION = b"version".ljust(12, b"\x00")
CMD_VERACK = b"verack".ljust(12, b"\x00")
CMD_GETADDR = b"getaddr".ljust(12, b"\x00")
CMD_ADDR = b"addr".ljust(12, b"\x00")
CMD_ADDRV2 = b"addrv2".ljust(12, b"\x00")
CMD_PING = b"ping".ljust(12, b"\x00")
CMD_PONG = b"pong".ljust(12, b"\x00")
CMD_REJECT = b"reject".ljust(12, b"\x00")
CMD_GETHEADERS = b"getheaders".ljust(12, b"\x00")
CMD_HEADERS = b"headers".ljust(12, b"\x00")

# Our crawler identity
CRAWLER_USER_AGENT = b"/knots-crawler:0.1.0/"
OUR_VERSION = 70016   # modern
OUR_SERVICES = NODE_NETWORK | NODE_WITNESS | NODE_NETWORK_LIMITED


def sha256d(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def checksum(data: bytes) -> bytes:
    return sha256d(data)[:4]


def varint(n: int) -> bytes:
    if n < 0xfd:
        return struct.pack("<B", n)
    elif n <= 0xffff:
        return b"\xfd" + struct.pack("<H", n)
    elif n <= 0xffffffff:
        return b"\xfe" + struct.pack("<I", n)
    else:
        return b"\xff" + struct.pack("<Q", n)


def read_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """Returns (value, new_offset)."""
    first = data[offset]
    if first < 0xfd:
        return first, offset + 1
    elif first == 0xfd:
        return struct.unpack("<H", data[offset+1:offset+3])[0], offset + 3
    elif first == 0xfe:
        return struct.unpack("<I", data[offset+1:offset+5])[0], offset + 5
    else:
        return struct.unpack("<Q", data[offset+1:offset+9])[0], offset + 9


def varstr(s: bytes) -> bytes:
    return varint(len(s)) + s


def read_varstr(data: bytes, offset: int = 0) -> Tuple[bytes, int]:
    length, offset = read_varint(data, offset)
    return data[offset:offset+length], offset + length


@dataclass
class NetAddr:
    """Bitcoin network address (used in addr and version)."""
    services: int
    ip: str          # human readable IPv4 or IPv6
    port: int

    def to_bytes_v1(self) -> bytes:
        """16-byte IP + 2-byte port (big endian), services 8 bytes LE."""
        try:
            ip_obj = ipaddress.ip_address(self.ip)
            if ip_obj.version == 4:
                ip_bytes = b"\x00" * 10 + b"\xff\xff" + ip_obj.packed
            else:
                ip_bytes = ip_obj.packed
        except ValueError:
            # fallback
            ip_bytes = b"\x00" * 16
        return struct.pack("<Q", self.services) + ip_bytes + struct.pack(">H", self.port)

    @classmethod
    def from_bytes_v1(cls, data: bytes, offset: int = 0) -> Tuple["NetAddr", int]:
        services = struct.unpack("<Q", data[offset:offset+8])[0]
        ip_bytes = data[offset+8:offset+24]
        port = struct.unpack(">H", data[offset+24:offset+26])[0]

        # Detect IPv4-in-IPv6
        if ip_bytes[:12] == b"\x00" * 10 + b"\xff\xff":
            ip = str(ipaddress.IPv4Address(ip_bytes[12:]))
        else:
            try:
                ip = str(ipaddress.IPv6Address(ip_bytes))
            except Exception:
                ip = ip_bytes.hex()
        return cls(services=services, ip=ip, port=port), offset + 26


@dataclass
class AddrV2Entry:
    """Compact addr v2 entry."""
    time: int
    services: int
    network_id: int
    addr: bytes   # raw, length depends on network_id
    port: int


class BitcoinProtocolError(Exception):
    pass


def build_message(command: bytes, payload: bytes, magic: bytes = MAGIC) -> bytes:
    """Build a full P2P message."""
    if len(command) != 12:
        raise ValueError("command must be exactly 12 bytes (padded)")
    length = len(payload)
    chksum = checksum(payload)
    return magic + command + struct.pack("<I", length) + chksum + payload


def parse_message(data: bytes) -> Tuple[bytes, bytes, int]:
    """
    Parse one message from the front of the buffer.
    Returns (command, payload, consumed_bytes) or raises if incomplete.
    Caller must handle partial data.
    """
    if len(data) < 24:
        raise BitcoinProtocolError("incomplete header")
    if data[:4] not in (MAGIC, MAGIC_TESTNET):
        raise BitcoinProtocolError(f"bad magic: {data[:4].hex()}")

    # Keep the full 12-byte padded command so it matches the CMD_* constants
    # (which are ljust-padded to 12 bytes and also used by build_message).
    command = data[4:16]
    length = struct.unpack("<I", data[16:20])[0]
    chksum = data[20:24]
    total = 24 + length

    if len(data) < total:
        raise BitcoinProtocolError("incomplete payload")

    payload = data[24:total]
    if checksum(payload) != chksum:
        raise BitcoinProtocolError("checksum mismatch")

    return command, payload, total


# ------------------------------------------------------------------
# Message builders
# ------------------------------------------------------------------

def build_version_payload(
    addr_recv: NetAddr,
    addr_from: NetAddr,
    nonce: Optional[int] = None,
    user_agent: bytes = CRAWLER_USER_AGENT,
    start_height: int = 0,
    relay: bool = False,
) -> bytes:
    """Construct the payload for 'version' message."""
    if nonce is None:
        import os as _os
        nonce = int.from_bytes(_os.urandom(8), "little")

    ts = int(time.time())
    payload = struct.pack(
        "<iQQ",  # version, services, timestamp
        OUR_VERSION, OUR_SERVICES, ts
    )
    payload += addr_recv.to_bytes_v1()
    payload += addr_from.to_bytes_v1()
    payload += struct.pack("<Q", nonce)
    payload += varstr(user_agent)
    payload += struct.pack("<i", start_height)
    payload += struct.pack("<?", relay)
    return payload


def build_verack() -> bytes:
    return build_message(CMD_VERACK, b"")


def build_getaddr() -> bytes:
    return build_message(CMD_GETADDR, b"")


def hash_display_to_internal(hex_hash: str) -> bytes:
    """Convert a display block hash (big-endian hex) to internal wire order
    (little-endian 32 bytes), as used inside getheaders locators and headers."""
    return bytes.fromhex(hex_hash)[::-1]


def build_getheaders(locator_hashes: List[bytes],
                     hash_stop: bytes = b"\x00" * 32,
                     version: int = OUR_VERSION) -> bytes:
    """Build a getheaders message. `locator_hashes` are 32-byte internal-order
    hashes; `hash_stop` zeroed means 'give me as many as you have'."""
    payload = struct.pack("<I", version)
    payload += varint(len(locator_hashes))
    for h in locator_hashes:
        assert len(h) == 32
        payload += h
    payload += hash_stop
    return build_message(CMD_GETHEADERS, payload)


def parse_headers_summary(payload: bytes, header_size: int = 80):
    """Summarize a `headers` message for chain-verified height measurement.

    Returns (count, first_prev_block, last_header) where first_prev_block is the
    prev_block field (internal order) of the first header — i.e. which locator the
    peer built its reply from — and last_header is the raw header of the last
    entry (compared byte-for-byte against our own tip header, which sidesteps the
    SHA256d-vs-BLAKE2b block-id question). Each entry is `header_size` header
    bytes + a tx_count varint (0 → 1 byte). header_size is 80 on Bitcoin but
    larger on the BLAKE2b chain (its PoW extends the header).
    """
    count, off = read_varint(payload, 0)
    if count == 0 or len(payload) < off + header_size:
        return 0, None, None
    first_prev = payload[off + 4: off + 36]
    stride = header_size + 1  # + tx_count varint (0)
    last_off = off + (count - 1) * stride
    last_header = payload[last_off: last_off + header_size] if len(payload) >= last_off + header_size else None
    return count, first_prev, last_header


def parse_headers_first_prevblock(payload: bytes) -> Optional[bytes]:
    """Return the prev_block field (internal order, 32 bytes) of the first
    header in a `headers` message, or None if the message is empty/short.
    We read prev_block rather than recomputing the block id, so this works
    regardless of the chain's PoW/id hash function (SHA256d vs BLAKE2b)."""
    count, off = read_varint(payload, 0)
    if count == 0 or len(payload) < off + 80:
        return None
    # Header layout: version(4) | prev_block(32) | merkle_root(32) | ...
    return payload[off + 4: off + 36]


def build_ping(nonce: Optional[int] = None) -> bytes:
    if nonce is None:
        import os as _os
        nonce = int.from_bytes(_os.urandom(8), "little")
    return build_message(CMD_PING, struct.pack("<Q", nonce))


def build_pong(nonce: int) -> bytes:
    return build_message(CMD_PONG, struct.pack("<Q", nonce))


# ------------------------------------------------------------------
# Message parsers (relevant for crawler)
# ------------------------------------------------------------------

def parse_version_payload(payload: bytes) -> dict:
    """Returns dict with interesting fields from version."""
    if len(payload) < 85:  # rough minimum
        raise BitcoinProtocolError("version payload too short")

    version, services, ts = struct.unpack("<iQQ", payload[:20])
    # skip addr_recv (26) + addr_from (26) = 52
    offset = 20 + 52
    nonce = struct.unpack("<Q", payload[offset:offset+8])[0]
    offset += 8

    user_agent, offset = read_varstr(payload, offset)
    start_height = struct.unpack("<i", payload[offset:offset+4])[0]
    offset += 4

    relay = False
    if len(payload) > offset:
        relay = bool(payload[offset])

    return {
        "version": version,
        "services": services,
        "timestamp": ts,
        "nonce": nonce,
        "user_agent": user_agent.decode("utf-8", errors="replace"),
        "start_height": start_height,
        "relay": relay,
    }


def parse_addr_payload(payload: bytes) -> List[NetAddr]:
    """
    Parse classic 'addr' message (v1).
    Returns list of NetAddr (timestamp is ignored for our purposes).
    """
    count, offset = read_varint(payload)
    addrs: List[NetAddr] = []
    for _ in range(count):
        # Each entry: 4 byte timestamp + 26 byte net_addr
        if offset + 30 > len(payload):
            break
        # ts = struct.unpack("<I", payload[offset:offset+4])[0]  # not needed
        offset += 4
        addr, offset = NetAddr.from_bytes_v1(payload, offset)
        addrs.append(addr)
    return addrs


def parse_addrv2_payload(payload: bytes) -> List[NetAddr]:
    """
    Parse 'addrv2' message (BIP155).
    We convert to the same NetAddr abstraction.
    """
    count, offset = read_varint(payload)
    addrs: List[NetAddr] = []

    for _ in range(min(count, 10000)):  # safety
        if offset + 4 > len(payload):
            break
        # time (4) + services (varint) + network_id (1) + addr_bytes (var) + port (2)
        _time = struct.unpack("<I", payload[offset:offset+4])[0]
        offset += 4

        services, offset = read_varint(payload, offset)

        if offset + 1 > len(payload):
            break
        network_id = payload[offset]
        offset += 1

        addr_len, offset = read_varint(payload, offset)
        if offset + addr_len + 2 > len(payload):
            break
        addr_raw = payload[offset:offset+addr_len]
        offset += addr_len
        port = struct.unpack(">H", payload[offset:offset+2])[0]
        offset += 2

        # Convert raw addr to string IP
        ip_str = _addrv2_raw_to_ip(network_id, addr_raw)
        if ip_str:
            addrs.append(NetAddr(services=services, ip=ip_str, port=port))

    return addrs


def _addrv2_raw_to_ip(network_id: int, raw: bytes) -> Optional[str]:
    # network_id values: https://github.com/bitcoin/bips/blob/master/bip-0155.mediawiki
    if network_id == 1:  # IPv4
        if len(raw) != 4:
            return None
        return str(ipaddress.IPv4Address(raw))
    elif network_id == 2:  # IPv6
        if len(raw) != 16:
            return None
        return str(ipaddress.IPv6Address(raw))
    elif network_id == 3:  # Tor v2 (ignore for crawler)
        return None
    elif network_id == 4:  # Tor v3
        return None
    elif network_id == 5:  # I2P
        return None
    elif network_id == 6:  # CJDNS
        return None
    else:
        # Unknown / future
        return None


# ------------------------------------------------------------------
# High level helpers used by crawler
# ------------------------------------------------------------------

async def read_exactly(reader: "asyncio.StreamReader", n: int, timeout: float = 30.0) -> bytes:
    """Read exactly n bytes with timeout."""
    import asyncio
    data = b""
    try:
        async with asyncio.timeout(timeout):
            while len(data) < n:
                chunk = await reader.read(n - len(data))
                if not chunk:
                    raise BitcoinProtocolError("connection closed while reading")
                data += chunk
    except asyncio.TimeoutError:
        raise BitcoinProtocolError(f"timeout reading {n} bytes") from None
    return data


async def read_message(reader: "asyncio.StreamReader", timeout: float = 30.0) -> Tuple[bytes, bytes]:
    """Read and parse one full message. Returns (command, payload)."""
    import asyncio
    # First read header
    header = await read_exactly(reader, 24, timeout)
    if header[:4] not in (MAGIC, MAGIC_TESTNET):
        raise BitcoinProtocolError(f"bad magic in header: {header[:4].hex()}")

    length = struct.unpack("<I", header[16:20])[0]
    if length > 4 * 1024 * 1024:  # 4MiB safety
        raise BitcoinProtocolError("payload too large")

    payload = await read_exactly(reader, length, timeout)
    chksum = header[20:24]
    if checksum(payload) != chksum:
        raise BitcoinProtocolError("checksum failed on received message")

    # Full 12-byte padded command, matching the CMD_* constants used for
    # both message construction and command comparison in the crawler.
    command = header[4:16]
    return command, payload


def make_version_message(addr_recv_ip: str, addr_recv_port: int = 8333,
                         addr_from_ip: str = "0.0.0.0", addr_from_port: int = 0,
                         start_height: int = 0) -> bytes:
    addr_recv = NetAddr(services=0, ip=addr_recv_ip, port=addr_recv_port)
    addr_from = NetAddr(services=OUR_SERVICES, ip=addr_from_ip, port=addr_from_port)
    payload = build_version_payload(
        addr_recv=addr_recv,
        addr_from=addr_from,
        user_agent=CRAWLER_USER_AGENT,
        start_height=start_height,
        relay=False,
    )
    return build_message(CMD_VERSION, payload)


# os.urandom is imported locally inside build_version_payload when needed.
