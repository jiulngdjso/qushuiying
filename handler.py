import os, json, time, uuid, subprocess
from pathlib import Path

import requests
import runpod

COMFY_DIR = Path("/comfyui")
API = "http://127.0.0.1:8188"
WORKFLOW_PATH = Path("/workflow_api.json")

INPUT_DIR = COMFY_DIR / "input"
OUTPUT_DIR = COMFY_DIR / "output"

def _log(*a):
    print("[handler]", *a, flush=True)

def ensure_models_symlink():
    """
    如果你在 Serverless Endpoint 里挂了 Network Volume 到 /runpod-volume，
    并把模型放在 /runpod-volume/models 下，这里会把常用子目录软链到 /comfyui/models/*
    """
    vol_models = Path("/runpod-volume/models")
    comfy_models = COMFY_DIR / "models"
    if not vol_models.exists():
        return
    comfy_models.mkdir(parents=True, exist_ok=True)

    subdirs = [
        "checkpoints", "diffusion_models", "text_encoders", "vae",
        "controlnet", "clip_vision", "loras", "ultralytics"
    ]
    for sd in subdirs:
        src = vol_models / sd
        dst = comfy_models / sd
        if not src.exists():
            continue
        try:
            if dst.is_symlink() or dst.exists():
                # 不强拆，避免误删镜像自带内容
                continue
            dst.symlink_to(src)
            _log("symlink models:", dst, "->", src)
        except Exception as e:
            _log("symlink failed:", sd, e)

def comfy_is_up() -> bool:
    try:
        r = requests.get(API + "/system_stats", timeout=1.5)
        return r.status_code == 200
    except Exception:
        return False

def start_comfyui():
    if comfy_is_up():
        return

    ensure_models_symlink()

    # 起 ComfyUI（API 服务）
    cmd = ["/opt/venv/bin/python", str(COMFY_DIR / "main.py"),
           "--listen", "0.0.0.0", "--port", "8188"]
    _log("starting ComfyUI:", " ".join(cmd))
    subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # 等它起来
    for _ in range(120):
        if comfy_is_up():
            _log("ComfyUI is up")
            return
        time.sleep(0.5)
    raise RuntimeError("ComfyUI failed to start")

def download(url: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return out_path

def load_workflow():
    data = json.loads(WORKFLOW_PATH.read_text("utf-8"))
    # 必须是 API format：{ "1": {"class_type":..., "inputs":...}, ... }
    if not isinstance(data, dict) or "nodes" in data:
        raise ValueError("workflow_api.json 不是 API 格式，请在 ComfyUI 里 Save (API Format) 导出")
    return data

def patch_workflow(prompt: dict, video_name: str, prefix: str):
    # 优先按 class_type 找节点
    def find_ids(ct):
        return [k for k, v in prompt.items() if v.get("class_type") == ct]

    load_ids = find_ids("VHS_LoadVideo")
    comb_ids = find_ids("VHS_VideoCombine")
    if not load_ids or not comb_ids:
        raise ValueError(f"没找到 VHS_LoadVideo / VHS_VideoCombine，请检查 workflow_api.json 是否正确")

    prompt[load_ids[0]]["inputs"]["video"] = video_name
    prompt[comb_ids[0]]["inputs"]["filename_prefix"] = prefix
    return prompt

def queue_prompt(prompt: dict):
    client_id = str(uuid.uuid4())
    body = {"client_id": client_id, "prompt": prompt}
    r = requests.post(API + "/prompt", json=body, timeout=60)
    r.raise_for_status()
    return r.json()["prompt_id"]

def wait_history(prompt_id: str, timeout_s: int = 3600):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        r = requests.get(API + "/history?max_items=50", timeout=30)
        r.raise_for_status()
        hist = r.json()
        if prompt_id in hist:
            return hist[prompt_id]
        time.sleep(1.0)
    raise TimeoutError("wait history timeout")

def pick_mp4_from_history(hist_item: dict) -> str:
    outputs = (hist_item or {}).get("outputs", {})
    for node_id, out in outputs.items():
        # 常见结构：{"videos":[{"filename": "...mp4", "subfolder":"", "type":"output"}]}
        for k in ("videos", "gifs", "images"):
            if k in out:
                for it in out[k]:
                    fn = it.get("filename", "")
                    if fn.lower().endswith(".mp4"):
                        sub = it.get("subfolder", "")
                        p = OUTPUT_DIR / sub / fn
                        return str(p)
    # 兜底：找最新 mp4
    mp4s = sorted(OUTPUT_DIR.rglob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if mp4s:
        return str(mp4s[0])
    raise FileNotFoundError("no mp4 output found")

def handler(job):
    start_comfyui()

    inp = job.get("input", {}) or {}
    video_url = inp.get("video_url")
    if not video_url:
        return {"error": "missing input.video_url"}

    job_id = job.get("id") or str(uuid.uuid4())
    video_name = f"{job_id}.mp4"
    in_path = INPUT_DIR / video_name
    prefix = f"job_{job_id}_"

    _log("downloading:", video_url)
    download(video_url, in_path)

    prompt = load_workflow()
    prompt = patch_workflow(prompt, video_name=video_name, prefix=prefix)

    _log("queue prompt...")
    pid = queue_prompt(prompt)
    _log("prompt_id:", pid)

    hist = wait_history(pid, timeout_s=int(inp.get("timeout_s", 3600)))
    out_mp4 = pick_mp4_from_history(hist)

    _log("done:", out_mp4)
    return {"prompt_id": pid, "output_mp4_path": out_mp4}

runpod.serverless.start({"handler": handler})

