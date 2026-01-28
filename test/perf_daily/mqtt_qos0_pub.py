#!/usr/bin/env python3

"""
Minimal MQTT v3.1.1 QoS0/QoS1 publisher implemented with stdlib only.

Copied and adapted from veloFlux perf harness to avoid pip dependencies in CI.
"""

from __future__ import annotations

import os
import socket
import struct
import time
from dataclasses import dataclass
from typing import Iterable, Optional


class MqttError(RuntimeError):
    pass


@dataclass(frozen=True)
class TcpBroker:
    host: str
    port: int


@dataclass(frozen=True)
class PublishResult:
    sent: int
    start_ts_ms: int
    end_ts_ms: int


def parse_tcp_broker_url(broker_url: str) -> TcpBroker:
    broker_url = broker_url.strip()
    if not broker_url.startswith("tcp://"):
        raise MqttError(f"unsupported broker_url scheme: {broker_url}")
    rest = broker_url[len("tcp://") :]
    if ":" not in rest:
        raise MqttError(f"broker_url missing port: {broker_url}")
    host, port_str = rest.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError as e:
        raise MqttError(f"invalid broker_url port: {broker_url}") from e
    if port <= 0 or port > 65535:
        raise MqttError(f"invalid broker_url port: {broker_url}")
    return TcpBroker(host=host, port=port)


def _encode_remaining_length(n: int) -> bytes:
    # MQTT "Remaining Length" variable encoding.
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n > 0:
            digit |= 0x80
        out.append(digit)
        if n == 0:
            break
    return bytes(out)


def _encode_utf8(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("!H", len(b)) + b


def _build_connect_packet(client_id: str, keepalive_secs: int = 60) -> bytes:
    # MQTT v3.1.1 CONNECT packet.
    proto_name = _encode_utf8("MQTT")
    proto_level = b"\x04"
    connect_flags = b"\x02"  # clean session
    keepalive = struct.pack("!H", keepalive_secs)
    payload = _encode_utf8(client_id)
    vh = proto_name + proto_level + connect_flags + keepalive
    remaining = _encode_remaining_length(len(vh) + len(payload))
    return b"\x10" + remaining + vh + payload


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise MqttError("socket closed while reading")
        buf.extend(chunk)
    return bytes(buf)


def _read_connack(sock: socket.socket) -> None:
    # CONNACK: 0x20 0x02 <ack_flags> <return_code>
    fixed = _read_exact(sock, 2)
    if fixed[0] != 0x20 or fixed[1] != 0x02:
        raise MqttError(f"unexpected CONNACK header: {fixed!r}")
    payload = _read_exact(sock, 2)
    rc = payload[1]
    if rc != 0:
        raise MqttError(f"CONNACK error return_code={rc}")


def _build_publish_packet(topic: str, payload: bytes) -> bytes:
    # QoS0, retain=false -> fixed header 0x30.
    vh = _encode_utf8(topic)
    remaining = _encode_remaining_length(len(vh) + len(payload))
    return b"\x30" + remaining + vh + payload


def _read_remaining_length(sock: socket.socket) -> int:
    multiplier = 1
    value = 0
    for _ in range(4):
        b = _read_exact(sock, 1)[0]
        value += (b & 127) * multiplier
        if (b & 128) == 0:
            return value
        multiplier *= 128
    raise MqttError("malformed Remaining Length")


def _read_control_packet(sock: socket.socket) -> tuple[int, bytes]:
    fixed1 = _read_exact(sock, 1)[0]
    rem = _read_remaining_length(sock)
    payload = _read_exact(sock, rem) if rem else b""
    return fixed1, payload


def _read_puback(sock: socket.socket, packet_id: int, timeout_secs: float) -> None:
    prev = sock.gettimeout()
    try:
        sock.settimeout(timeout_secs)
        while True:
            fixed1, payload = _read_control_packet(sock)
            # PUBACK fixed header: 0x40, remaining length 0x02, payload: packet id.
            if fixed1 != 0x40:
                continue
            if len(payload) != 2:
                continue
            got = struct.unpack("!H", payload)[0]
            if got == packet_id:
                return
    except socket.timeout as e:
        raise MqttError(f"PUBACK timeout after {timeout_secs}s") from e
    finally:
        sock.settimeout(prev)


def _build_publish_packet_qos1(topic: str, payload: bytes, packet_id: int) -> bytes:
    # QoS1, retain=false, dup=false -> fixed header 0x32.
    vh = _encode_utf8(topic) + struct.pack("!H", packet_id)
    remaining = _encode_remaining_length(len(vh) + len(payload))
    return b"\x32" + remaining + vh + payload


def publish_qos0_with_timing(
    broker_url: str,
    topic: str,
    payloads: Iterable[bytes],
    publish_count: int = 0,
    duration_secs: int = 0,
    rate_per_sec: int = 0,
    client_id: Optional[str] = None,
    connect_timeout_secs: float = 10.0,
    keepalive_secs: int = 60,
) -> PublishResult:
    if publish_count <= 0 and duration_secs <= 0:
        return PublishResult(sent=0, start_ts_ms=int(time.time() * 1000), end_ts_ms=int(time.time() * 1000))
    payload_list = list(payloads)
    if not payload_list:
        raise MqttError("payloads is empty")

    broker = parse_tcp_broker_url(broker_url)
    if client_id is None:
        client_id = f"perf-daily-{os.getpid()}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sent = 0
    start_ts_ms = 0
    end_ts_ms = 0
    try:
        sock.settimeout(connect_timeout_secs)
        sock.connect((broker.host, broker.port))
        sock.settimeout(None)
        sock.sendall(_build_connect_packet(client_id, keepalive_secs=keepalive_secs))
        _read_connack(sock)

        start = time.time()
        start_ts_ms = int(start * 1000)
        i = 0
        while True:
            now = time.time()
            if duration_secs > 0 and (now - start) >= duration_secs:
                break
            if duration_secs <= 0 and i >= publish_count:
                break

            payload = payload_list[i % len(payload_list)]
            sock.sendall(_build_publish_packet(topic, payload))
            sent += 1
            i += 1

            if rate_per_sec > 0:
                target = start + (i / float(rate_per_sec))
                delay = target - time.time()
                if delay > 0:
                    time.sleep(delay)
        end_ts_ms = int(time.time() * 1000)
    finally:
        try:
            sock.close()
        except Exception:
            pass

    if start_ts_ms <= 0:
        start_ts_ms = int(time.time() * 1000)
    if end_ts_ms <= 0:
        end_ts_ms = int(time.time() * 1000)
    return PublishResult(sent=sent, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)


def publish_qos1_with_timing(
    broker_url: str,
    topic: str,
    payloads: Iterable[bytes],
    duration_secs: int,
    rate_per_sec: int,
    client_id: Optional[str] = None,
    connect_timeout_secs: float = 10.0,
    keepalive_secs: int = 60,
    puback_timeout_secs: float = 5.0,
) -> PublishResult:
    if duration_secs <= 0 or rate_per_sec <= 0:
        now = int(time.time() * 1000)
        return PublishResult(sent=0, start_ts_ms=now, end_ts_ms=now)

    payload_list = list(payloads)
    if not payload_list:
        raise MqttError("payloads is empty")

    broker = parse_tcp_broker_url(broker_url)
    if client_id is None:
        client_id = f"perf-daily-{os.getpid()}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sent = 0
    start_ts_ms = 0
    end_ts_ms = 0
    try:
        sock.settimeout(connect_timeout_secs)
        sock.connect((broker.host, broker.port))
        sock.settimeout(None)
        sock.sendall(_build_connect_packet(client_id, keepalive_secs=keepalive_secs))
        _read_connack(sock)

        start = time.time()
        start_ts_ms = int(start * 1000)
        i = 0
        packet_id = 1
        while True:
            now = time.time()
            if (now - start) >= duration_secs:
                break

            payload = payload_list[i % len(payload_list)]
            sock.sendall(_build_publish_packet_qos1(topic, payload, packet_id=packet_id))
            _read_puback(sock, packet_id=packet_id, timeout_secs=puback_timeout_secs)
            sent += 1
            i += 1
            packet_id += 1
            if packet_id > 65535:
                packet_id = 1

            target = start + (i / float(rate_per_sec))
            delay = target - time.time()
            if delay > 0:
                time.sleep(delay)
        end_ts_ms = int(time.time() * 1000)
    finally:
        try:
            sock.close()
        except Exception:
            pass

    if start_ts_ms <= 0:
        start_ts_ms = int(time.time() * 1000)
    if end_ts_ms <= 0:
        end_ts_ms = int(time.time() * 1000)
    return PublishResult(sent=sent, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)

