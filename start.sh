#!/usr/bin/env bash
set -e

cd "${COMFYUI_DIR:-/comfyui}"

# 启动 ComfyUI API（只监听容器内部）
/opt/venv/bin/python main.py \
  --listen 127.0.0.1 \
  --port "${COMFYUI_PORT:-8188}" \
  --disable-auto-launch &

# 等 API ready
/opt/venv/bin/python - <<'PY'
import time, requests, os
url=f"http://127.0.0.1:{os.getenv('COMFYUI_PORT','8188')}/system_stats"
for i in range(120):
    try:
        r=requests.get(url, timeout=1)
        if r.ok:
            print("ComfyUI ready")
            break
    except Exception:
        pass
    time.sleep(1)
else:
    raise SystemExit("ComfyUI not ready")
PY

# 启动 RunPod handler
/opt/venv/bin/python -u /handler.py

