#!/usr/bin/env python3
"""BrowserStack live-device viewer: receives the device screen over WebRTC and re-streams it.

Signaling (reverse-engineered from the dashboard, socket.io EIO=3 over wss):
  client -> customMessage, init(screenPeer), init(dataPeer)
  server -> offer(screenPeer, H264) + fullcandidate*, offer(dataPeer) + fullcandidate*
  client -> answer(screenPeer), answer(dataPeer)

Outputs:
  http://HOST:PORT/stream.mjpg   live MJPEG (VLC, ffplay, <img>, OpenCV)
  http://HOST:PORT/snapshot.jpg  latest frame
  http://HOST:PORT/              tiny viewer page
  http://HOST:PORT/status        JSON stats
  --out URL                      push to any ffmpeg target (rtsp://, rtmp://, srt://, udp://, file.mp4)
"""
import argparse
import asyncio
import json
import logging
import os
import shutil
import time
from urllib.parse import urlencode

import cv2
import websockets
from aiohttp import web
from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp

log = logging.getLogger("rtc-strem")

SIGNAL_HOST = "wss://peer-alb-aps1-prod.browserstack.com/socket.io/"
ORIGIN = "https://app-automate.browserstack.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36")
STUN = [
    "stun:turn-live-007-dcm-aps1c-prod.browserstack.com:443",
    "stun:turn-live-008-dcm-aps1c-prod.browserstack.com:443",
    "stun:turn-live-006-ec2-aps1b-prod.browserstack.com:443",
    "stun:turn-lse-001-ec2-aps1a-prod.browserstack.com:443",
    "stun:stun.l.google.com:19302",
]


class FrameSink:
    """Holds the latest decoded frame and feeds the HTTP + ffmpeg outputs."""

    def __init__(self, fps, quality):
        self.fps = fps
        self.quality = quality
        self.frame = None          # latest BGR ndarray
        self.seq = 0
        self.count = 0
        self.first_at = None
        self.last_at = None
        self._jpeg = (None, -1)    # (bytes, seq it was encoded from)
        self._win = []             # arrival times for fps estimate

    def push(self, av_frame):
        self.frame = av_frame.to_ndarray(format="bgr24")
        self.seq += 1
        self.count += 1
        now = time.time()
        self.first_at = self.first_at or now
        self.last_at = now
        self._win.append(now)
        while self._win and now - self._win[0] > 5:
            self._win.pop(0)

    def jpeg(self):
        if self.frame is None:
            return None
        data, seq = self._jpeg
        if seq != self.seq:
            ok, buf = cv2.imencode(".jpg", self.frame, [cv2.IMWRITE_JPEG_QUALITY, self.quality])
            if not ok:
                return data
            data = buf.tobytes()
            self._jpeg = (data, self.seq)
        return data

    def recv_fps(self):
        return round(len(self._win) / 5.0, 1) if self._win else 0.0

    def status(self):
        h, w = (self.frame.shape[:2]) if self.frame is not None else (0, 0)
        return {
            "frames": self.count, "width": w, "height": h, "recv_fps": self.recv_fps(),
            "last_frame_age_s": round(time.time() - self.last_at, 2) if self.last_at else None,
        }


class FfmpegOut:
    """Pushes frames at a constant rate to an ffmpeg target; restarts if the frame size changes."""

    def __init__(self, sink, url, fps):
        self.sink, self.url, self.fps = sink, url, fps
        self.proc = None
        self.size = None

    def _cmd(self, w, h):
        u = self.url.lower()
        if u.startswith("rtsp://"):
            tail = ["-f", "rtsp", "-rtsp_transport", "tcp", self.url]
        elif u.startswith(("rtmp://", "rtmps://")):
            tail = ["-f", "flv", self.url]
        elif u.startswith(("srt://", "udp://", "tcp://")):
            tail = ["-f", "mpegts", self.url]
        else:
            tail = [self.url]
        return [shutil.which("ffmpeg") or "ffmpeg", "-loglevel", "warning", "-y",
                "-f", "rawvideo", "-pix_fmt", "bgr24", "-s", f"{w}x{h}", "-r", str(self.fps), "-i", "-",
                "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-pix_fmt", "yuv420p", "-g", str(self.fps * 2)] + tail

    async def _start(self, w, h):
        await self._stop()
        self.size = (w, h)
        log.info("ffmpeg out -> %s (%dx%d @ %d fps)", self.url, w, h, self.fps)
        self.proc = await asyncio.create_subprocess_exec(*self._cmd(w, h), stdin=asyncio.subprocess.PIPE)

    async def _stop(self):
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.stdin.close()
                await asyncio.wait_for(self.proc.wait(), 5)
            except Exception:
                self.proc.kill()
        self.proc = None

    async def run(self):
        period = 1.0 / self.fps
        while True:
            await asyncio.sleep(period)
            f = self.sink.frame
            if f is None:
                continue
            h, w = f.shape[:2]
            w, h = w - w % 2, h - h % 2          # libx264 needs even dimensions
            if self.proc is None or self.proc.returncode is not None or self.size != (w, h):
                await self._start(w, h)
            try:
                self.proc.stdin.write(f[:h, :w].tobytes())
                await self.proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                self.proc = None


class Viewer:
    def __init__(self, args, sink):
        self.a = args
        self.sink = sink
        self.pcs = {}
        self.pending = {}
        self.ws = None
        self.state = "idle"
        self.failed = asyncio.Event()
        ice = [RTCIceServer(urls=STUN)]
        if args.turn:
            ice.append(RTCIceServer(urls=args.turn, username=args.turn_user, credential=args.turn_pass))
        self.rtc_config = RTCConfiguration(iceServers=ice)

    # ---- signaling transport (socket.io EIO=3) ----
    def _url(self):
        return SIGNAL_HOST + "?" + urlencode(dict(
            tag="wspool", uid=self.a.uid, js="v71.16", browser="chrome", os="win",
            subdomain="app-automate", iov="IO201", EIO="3", transport="websocket"))

    async def _emit(self, event, payload):
        await self.ws.send("42" + json.dumps([event, payload]))

    def _msg(self, peer, typ, message):
        ch = self.a.session + ("_data" if peer == "dataPeer" else "")
        return {"type": typ, "message": message, "channel": ch, "user_id": int(self.a.uid),
                "sender": "client:", "peer_type": peer, "live_session_id": self.a.session}

    async def run_once(self):
        self.failed.clear()
        self.pcs, self.pending = {}, {}
        self.state = "signaling"
        async with websockets.connect(self._url(), origin=ORIGIN, user_agent_header=UA,
                                      max_size=None, ping_interval=None, open_timeout=20) as ws:
            self.ws = ws
            hello = await ws.recv()
            interval = json.loads(hello[1:]).get("pingInterval", 25000) / 1000.0
            log.info("signaling connected (ping every %.0fs)", interval)
            pinger = asyncio.create_task(self._ping(interval))
            reader = asyncio.create_task(self._read())
            try:
                await asyncio.sleep(0.5)
                await self._emit("message", {"type": "customMessage", "message": "webrtc client connected",
                                             "channel": {}, "user_id": int(self.a.uid), "sender": "client:"})
                await asyncio.sleep(0.3)
                await self._emit("message", self._msg("screenPeer", "init", "{}"))
                await self._emit("message", self._msg("dataPeer", "init", "{}"))
                done, _ = await asyncio.wait({reader, asyncio.create_task(self.failed.wait())},
                                             return_when=asyncio.FIRST_COMPLETED)
            finally:
                pinger.cancel()
                reader.cancel()
                for pc in self.pcs.values():
                    await pc.close()
                self.state = "idle"

    async def _ping(self, interval):
        while True:
            await asyncio.sleep(interval)
            await self.ws.send("2")

    async def _read(self):
        async for raw in self.ws:
            if not isinstance(raw, str) or not raw.startswith("42"):
                continue
            event, payload = json.loads(raw[2:])
            if event == "server_info":
                log.info("server_info: %s", payload)
            elif event == "message" and isinstance(payload, dict):
                if payload.get("sender") == "server:":
                    await self._on_server(payload)
            else:
                log.debug("event %s", event)

    async def _on_server(self, d):
        peer, typ = d.get("peer_type"), d.get("type")
        if peer not in ("screenPeer", "dataPeer"):
            log.info("server message type=%s %s", typ, str(d)[:200])
            return
        if typ == "offer" or str(d.get("message", "")).startswith("v=0"):
            await self._on_offer(peer, d)
        elif typ == "fullcandidate":
            await self._on_candidate(peer, d)
        else:
            log.info("server %s type=%s %s", peer, typ, str(d.get("message"))[:200])

    # ---- WebRTC ----
    async def _on_offer(self, peer, d):
        log.info("offer received for %s", peer)
        pc = RTCPeerConnection(self.rtc_config)
        self.pcs[peer] = pc

        @pc.on("connectionstatechange")
        async def _state():
            log.info("%s connection: %s", peer, pc.connectionState)
            if peer == "screenPeer":
                self.state = pc.connectionState
                if pc.connectionState in ("failed", "closed"):
                    self.failed.set()

        @pc.on("track")
        def _track(track):
            log.info("%s track: %s", peer, track.kind)
            if track.kind == "video":
                asyncio.ensure_future(self._consume(track))

        @pc.on("datachannel")
        def _dc(ch):
            log.info("datachannel open: %s", ch.label)

            @ch.on("message")
            def _m(m):
                log.debug("datachannel %s: %s", ch.label, str(m)[:200])

        await pc.setRemoteDescription(RTCSessionDescription(sdp=d["message"], type="offer"))
        for t in pc.getTransceivers():
            if t.kind == "video":
                t.direction = "recvonly"
        await pc.setLocalDescription(await pc.createAnswer())
        await self._emit("message", self._msg(peer, "answer", pc.localDescription.sdp))
        log.info("answer sent for %s", peer)
        for c in self.pending.pop(peer, []):
            await self._add_candidate(pc, c)

    async def _on_candidate(self, peer, d):
        pc = self.pcs.get(peer)
        if pc is None or pc.remoteDescription is None:
            self.pending.setdefault(peer, []).append(d)
        else:
            await self._add_candidate(pc, d)

    async def _add_candidate(self, pc, d):
        try:
            c = json.loads(d["message"]) if isinstance(d["message"], str) else d["message"]
            line = c.get("candidate") or ""
            if not line:
                return
            ice = candidate_from_sdp(line.split(":", 1)[1] if line.startswith("candidate:") else line)
            ice.sdpMid, ice.sdpMLineIndex = c.get("sdpMid"), c.get("sdpMLineIndex")
            await pc.addIceCandidate(ice)
        except Exception as e:  # a bad candidate must not kill the session
            log.warning("candidate skipped: %s", e)

    async def _consume(self, track):
        try:
            while True:
                self.sink.push(await track.recv())
                if self.sink.count == 1:
                    log.info("FIRST FRAME received (%dx%d)", self.sink.frame.shape[1], self.sink.frame.shape[0])
        except Exception as e:
            log.info("video track ended: %s", e)
            self.failed.set()


# ---- HTTP ----
INDEX = """<!doctype html><meta charset=utf-8><title>rtc-strem</title>
<body style="margin:0;background:#111;display:flex;justify-content:center">
<img src="/stream.mjpg" style="height:100vh"></body>"""


def make_app(sink, viewer):
    async def index(_):
        return web.Response(text=INDEX, content_type="text/html")

    async def snapshot(_):
        j = sink.jpeg()
        if j is None:
            return web.Response(status=503, text="no frame yet")
        return web.Response(body=j, content_type="image/jpeg", headers={"Cache-Control": "no-store"})

    async def mjpeg(request):
        resp = web.StreamResponse(headers={
            "Content-Type": "multipart/x-mixed-replace; boundary=frame", "Cache-Control": "no-store"})
        await resp.prepare(request)
        period = 1.0 / sink.fps
        try:
            while True:
                j = sink.jpeg()
                if j:
                    await resp.write(b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: "
                                     + str(len(j)).encode() + b"\r\n\r\n" + j + b"\r\n")
                await asyncio.sleep(period)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return resp

    async def status(_):
        return web.json_response({"state": viewer.state, **sink.status()})

    app = web.Application()
    app.add_routes([web.get("/", index), web.get("/snapshot.jpg", snapshot),
                    web.get("/stream.mjpg", mjpeg), web.get("/status", status)])
    return app


async def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--session", default=os.environ.get("BS_SESSION_ID"),
                   help="BrowserStack session id (the long hex id in the dashboard URL .../sessions/<id>)")
    p.add_argument("--uid", default=os.environ.get("BS_USER_ID"),
                   help="BrowserStack numeric user id (the uid= value on the dashboard's wspool socket)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--fps", type=int, default=15, help="output frame rate for MJPEG / ffmpeg")
    p.add_argument("--quality", type=int, default=80, help="MJPEG JPEG quality")
    p.add_argument("--out", help="also push to an ffmpeg target, e.g. rtsp://127.0.0.1:8554/phone")
    p.add_argument("--turn", help="optional TURN url, e.g. turn:host:443?transport=tcp")
    p.add_argument("--turn-user")
    p.add_argument("--turn-pass")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()
    if not a.session or not a.uid:
        p.error("--session and --uid are required (or set BS_SESSION_ID / BS_USER_ID)")
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("aioice").setLevel(logging.WARNING)
    logging.getLogger("aiortc").setLevel(logging.WARNING)

    sink = FrameSink(a.fps, a.quality)
    viewer = Viewer(a, sink)
    runner = web.AppRunner(make_app(sink, viewer))
    await runner.setup()
    await web.TCPSite(runner, a.host, a.port).start()
    log.info("stream URL:  http://%s:%d/stream.mjpg   (viewer: http://%s:%d/)", a.host, a.port, a.host, a.port)

    tasks = []
    if a.out:
        tasks.append(asyncio.create_task(FfmpegOut(sink, a.out, a.fps).run()))

    async def report():
        while True:
            await asyncio.sleep(5)
            log.info("state=%s %s", viewer.state, sink.status())
    tasks.append(asyncio.create_task(report()))

    backoff = 2
    while True:
        try:
            await viewer.run_once()
            backoff = 2
        except Exception as e:
            log.warning("session ended: %r", e)
        log.info("reconnecting in %ds", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
