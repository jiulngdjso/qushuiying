FROM runpod/worker-comfyui:5.5.1-base

WORKDIR /comfyui

# 1) 复制：依赖锁 + 工作流(API格式) + handler
COPY requirements.added.lock.txt /requirements.added.lock.txt
COPY workflow_api.json /workflow_api.json
COPY handler.py /handler.py

# 2) 安装新增 pip 依赖（用 base 镜像自带的 venv）
RUN /opt/venv/bin/pip install -U pip \
 && /opt/venv/bin/pip install -r /requirements.added.lock.txt

# 3) 安装工作流需要的 custom_nodes（先用“拉最新”跑通；后面你再 pin commit）
RUN set -eux; \
  cd /comfyui/custom_nodes; \
  git clone --depth 1 https://github.com/ltdrdata/ComfyUI-Manager || true; \
  git clone --depth 1 https://github.com/Kijai/ComfyUI-WanVideoWrapper || true; \
  git clone --depth 1 https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite || true; \
  git clone --depth 1 https://github.com/ltdrdata/ComfyUI-Impact-Pack || true; \
  git clone --depth 1 https://github.com/ltdrdata/ComfyUI-Impact-Subpack || true; \
  git clone --depth 1 https://github.com/chflame163/ComfyUI_LayerStyle || true;

ENV PYTHONUNBUFFERED=1
CMD ["/opt/venv/bin/python", "-u", "/handler.py"]

