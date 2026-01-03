FROM runpod/worker-comfyui:5.5.1-base

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg git && \
    rm -rf /var/lib/apt/lists/*

# ComfyUI 基座一般在 /comfyui
ENV COMFYUI_DIR=/comfyui
ENV COMFYUI_PORT=8188
ENV PYTHONUNBUFFERED=1

WORKDIR /comfyui

# 1) 安装你“跑通工作流”的最小依赖
COPY locks/requirements.lock.txt /tmp/requirements.lock.txt
RUN /opt/venv/bin/python -m pip install --no-cache-dir -r /tmp/requirements.lock.txt

# 2) 安装自定义节点（按 lock 固定版本）
COPY locks/custom_nodes.lock.txt /tmp/custom_nodes.lock.txt
COPY tools/install_custom_nodes.py /tmp/install_custom_nodes.py
RUN /opt/venv/bin/python /tmp/install_custom_nodes.py \
      --lock /tmp/custom_nodes.lock.txt \
      --dst /comfyui/custom_nodes

# 3) 拷贝工作流 + handler
COPY workflows/workflow_api.json /comfyui/workflows/workflow_api.json
COPY handler.py /handler.py
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
