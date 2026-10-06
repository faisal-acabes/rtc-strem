# rtc-strem

Live BrowserStack App Automate device screen, received over WebRTC from BrowserStack's signaling server.

**Download:** https://github.com/faisal-acabes/rtc-strem/releases/latest/download/bs-live-viewer.html (single file, open it in Chrome/Brave/Edge)

## index.html (static, no server)
Open the file in Chrome/Brave/Edge, fill in the session id and uid, press Connect.
Or pre-fill: `index.html?autoconnect&session=<id>&uid=<uid>`

- session id: the hex id in the dashboard URL `.../sessions/<id>`
- uid: the `uid=` value on the dashboard's `socket.io/?tag=wspool` websocket

## streamer.py (headless, re-streams to non-browser clients)
    python -m venv .venv && .venv\Scripts\pip install -r requirements.txt
    .venv\Scripts\python streamer.py --session <id> --uid <uid>
Serves `http://127.0.0.1:8090/stream.mjpg` (VLC/ffplay/OpenCV), `/snapshot.jpg`, `/status`.
`--out rtsp://...|rtmp://...|srt://...` also pushes via ffmpeg.

## Notes
- Only one viewer per session: close the dashboard tab (and any other viewer) first, or no offer arrives.
- iOS sends frames only when the screen changes, so a static screen shows 0 fps.
- Unofficial: this reuses BrowserStack's internal dashboard protocol and may break without notice.

## Hosted copy
https://faisal-acabes.github.io/rtc-strem/ (GitHub Pages; the page runs entirely in your browser and talks to BrowserStack directly, nothing goes through a server of ours).
