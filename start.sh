#!/usr/bin/env bash
set -euo pipefail

COMFYUI_DIR="${COMFYUI_DIR:-/comfyui}"
COMFYUI_PORT="${COMFYUI_PORT:-8188}"

# 0) 把 network volume 的模型目录映射到 ComfyUI 默认模型目录
if [ -d "/runpod-volume/models" ]; then
  echo "[BOOT] linking /comfyui/models -> /runpod-volume/models"
  rm -rf "${COMFYUI_DIR}/models"
  ln -s /runpod-volume/models "${COMFYUI_DIR}/models"
fi

cd "${COMFYUI_DIR}"

# 1) 启动 ComfyUI（只监听容器内部）
/opt/venv/bin/python main.py \
  --listen 127.0.0.1 \
  --port "${COMFYUI_PORT}" \
  --disable-auto-launch &

# 2) 等 ComfyUI API ready
/opt/venv/bin/python - <<'PY'
import time, os, requests
port=os.getenv("COMFYUI_PORT","8188")
url=f"http://127.0.0.1:{port}/system_stats"
for i in range(180):
    try:
        r=requests.get(url, timeout=1)
        if r.ok:
            print("[BOOT] ComfyUI ready")
            break
    except Exception:
        pass
    time.sleep(1)
else:
    raise SystemExit("[BOOT] ComfyUI not ready")
PY

# 3) 启动 RunPod handler
exec /opt/venv/bin/python -u /handler.py
