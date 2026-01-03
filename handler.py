import os
import json
import time
import uuid
import shutil
import subprocess
from fractions import Fraction
from typing import Any, Dict, Tuple, Optional

import boto3
import requests
import runpod


# -----------------------------
# Config
# -----------------------------
COMFY_HOST = os.environ.get("COMFY_HOST", "127.0.0.1")
COMFY_PORT = int(os.environ.get("COMFYUI_PORT") or os.environ.get("COMFY_PORT") or "8188")
COMFY_BASE = f"http://{COMFY_HOST}:{COMFY_PORT}"

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")

# 你的策略（按你最新要求）
FPS_CAP = 30          # 只要输入 fps > 30 才降到 30
MAX_SECONDS = 15      # 只要时长 > 15 秒才裁剪到前 15 秒

WORKFLOW_PATH = "/comfyui/workflows/workflow_api.json"
COMFY_OUTPUT_DIR = "/comfyui/output"


# -----------------------------
# Helpers
# -----------------------------
def _require_env(name: str, val: Optional[str]):
    if not val:
        raise RuntimeError(f"Missing env var: {name}")


def deep_replace(obj: Any, mapping: Dict[str, str]) -> Any:
    """Recursively replace placeholder strings inside JSON-like structure."""
    if isinstance(obj, dict):
        return {k: deep_replace(v, mapping) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_replace(v, mapping) for v in obj]
    if isinstance(obj, str):
        for k, v in mapping.items():
            obj = obj.replace(k, v)
        return obj
    return obj


def sh(cmd: list, check: bool = True) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nSTDERR:\n{p.stderr}")
    return p.stdout.strip()


def probe_video(path: str) -> Tuple[Optional[float], Optional[float]]:
    """Returns (fps, duration_seconds) using ffprobe."""
    out = sh([
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=avg_frame_rate,r_frame_rate",
        "-show_entries", "format=duration",
        "-of", "json",
        path
    ])
    try:
        data = json.loads(out)

        fps = None
        stream = (data.get("streams") or [{}])[0]
        afr = stream.get("avg_frame_rate") or "0/0"
        rfr = stream.get("r_frame_rate") or "0/0"
        for frac in (afr, rfr):
            try:
                f = Fraction(frac)
                if f.numerator and f.denominator:
                    fps = float(f)
                    break
            except Exception:
                pass

        dur = None
        fmt = data.get("format") or {}
        if fmt.get("duration") is not None:
            try:
                dur = float(fmt["duration"])
            except Exception:
                dur = None

        return fps, dur
    except Exception:
        return None, None


def ffmpeg_trim_copy(src: str, dst: str, max_seconds: int) -> Dict[str, Any]:
    """
    Trim to first max_seconds with stream copy (no re-encode).
    Keeps quality.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", src,
        "-t", str(int(max_seconds)),
        "-c", "copy",
        "-movflags", "+faststart",
        "-avoid_negative_ts", "make_zero",
        dst
    ]
    sh(cmd, check=True)
    fps_out, dur_out = probe_video(dst)
    return {"path": dst, "fps_out": fps_out, "dur_out": dur_out, "transcoded": False, "remuxed": True}


def ffmpeg_downsample_encode(src: str, dst: str, fps_cap: int, max_seconds: Optional[int]) -> Dict[str, Any]:
    """
    Downsample fps (requires re-encode). Optionally trim.
    Try audio copy first; fallback to AAC if needed.
    """
    base = ["ffmpeg", "-y", "-i", src]
    if max_seconds and max_seconds > 0:
        base += ["-t", str(int(max_seconds))]

    base += ["-vf", f"fps={int(fps_cap)}"]

    # Try audio copy first
    cmd1 = base + [
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-c:a", "copy",
        "-movflags", "+faststart",
        dst
    ]

    try:
        sh(cmd1, check=True)
    except Exception:
        cmd2 = base + [
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-movflags", "+faststart",
            dst
        ]
        sh(cmd2, check=True)

    fps_out, dur_out = probe_video(dst)
    return {"path": dst, "fps_out": fps_out, "dur_out": dur_out, "transcoded": True, "remuxed": False}


def preprocess_video(src: str, dst: str, fps_cap: int, max_seconds: int) -> Dict[str, Any]:
    """
    Rules:
    - if fps_in > FPS_CAP -> downsample to FPS_CAP (re-encode)
    - if dur_in > MAX_SECONDS -> trim to MAX_SECONDS
    - if only trim needed -> stream copy (no re-encode)
    - if nothing needed -> use src directly
    """
    fps_in, dur_in = probe_video(src)

    need_fps = (fps_in is not None and fps_in > (fps_cap + 0.01))
    need_trim = (dur_in is not None and dur_in > (max_seconds + 0.001))

    info = {
        "fps_in": fps_in,
        "dur_in": dur_in,
        "fps_cap": fps_cap,
        "max_seconds": max_seconds,
        "downsampled": False,
        "trimmed": False,
        "transcoded": False,
        "remuxed": False,
        "path": src
    }

    if not need_fps and not need_trim:
        return info

    # only trim -> no re-encode
    if need_trim and not need_fps:
        out = ffmpeg_trim_copy(src, dst, max_seconds=max_seconds)
        info.update(out)
        info["trimmed"] = True
        return info

    # need fps downsample -> re-encode (and trim if needed)
    out = ffmpeg_downsample_encode(src, dst, fps_cap=fps_cap, max_seconds=(max_seconds if need_trim else None))
    info.update(out)
    info["downsampled"] = True
    info["transcoded"] = True
    if need_trim:
        info["trimmed"] = True
    return info


def comfy_post_prompt(prompt: dict) -> str:
    r = requests.post(f"{COMFY_BASE}/prompt", json={"prompt": prompt}, timeout=60)
    r.raise_for_status()
    return r.json()["prompt_id"]


def comfy_get_history(prompt_id: str) -> dict:
    r = requests.get(f"{COMFY_BASE}/history/{prompt_id}", timeout=60)
    r.raise_for_status()
    return r.json()


def wait_until_done(prompt_id: str, timeout_sec: int = 3600, poll: float = 1.0) -> dict:
    t0 = time.time()
    while True:
        hist = comfy_get_history(prompt_id)
        if prompt_id in hist:
            status = hist[prompt_id].get("status", {})
            if status.get("completed", False) or status.get("status_str") == "success":
                return hist[prompt_id]
            if status.get("status_str") == "error":
                raise RuntimeError(f"ComfyUI error: {json.dumps(status, ensure_ascii=False)}")
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(f"ComfyUI job timeout after {timeout_sec}s, prompt_id={prompt_id}")
        time.sleep(poll)


def find_latest_output(prefix: str) -> str:
    best = None
    best_mtime = -1
    if not os.path.isdir(COMFY_OUTPUT_DIR):
        raise RuntimeError(f"Missing output dir: {COMFY_OUTPUT_DIR}")
    for fn in os.listdir(COMFY_OUTPUT_DIR):
        if not fn.startswith(prefix):
            continue
        if not fn.lower().endswith(".mp4"):
            continue
        p = os.path.join(COMFY_OUTPUT_DIR, fn)
        try:
            mt = os.path.getmtime(p)
            if mt > best_mtime:
                best_mtime = mt
                best = p
        except Exception:
            continue
    if not best:
        raise RuntimeError(f"No output mp4 found for prefix={prefix} in {COMFY_OUTPUT_DIR}")
    return best


# -----------------------------
# Main handler
# -----------------------------
def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    _require_env("S3_ENDPOINT_URL", S3_ENDPOINT_URL)
    _require_env("S3_BUCKET", S3_BUCKET)
    _require_env("S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID)
    _require_env("S3_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY)

    inp = (event or {}).get("input") or {}
    input_key = inp.get("input_key")
    if not input_key:
        return {"error": "missing input.input_key"}

    job_id = inp.get("job_id") or uuid.uuid4().hex[:12]
    output_key = inp.get("output_key") or f"outputs/{job_id}.mp4"

    # 只允许传 timeout_sec；fps/时长按固定策略执行（避免误传 fps_limit 造成抽帧）
    params = inp.get("params") or {}
    timeout_sec = int(params.get("timeout_sec", 3600))

    workdir = f"/tmp/jobs/{job_id}"
    os.makedirs(workdir, exist_ok=True)

    local_in = os.path.join(workdir, "in.mp4")
    local_pre = os.path.join(workdir, "in_pre.mp4")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )

    t0 = time.time()
    try:
        # 1) Download from R2
        s3.download_file(S3_BUCKET, input_key, local_in)
        t_dl = time.time()

        # 2) Preprocess with fixed rules
        pre_info = preprocess_video(local_in, local_pre, fps_cap=FPS_CAP, max_seconds=MAX_SECONDS)
        input_path_for_comfy = pre_info["path"]
        t_pre = time.time()

        # 3) Load workflow and replace placeholders
        with open(WORKFLOW_PATH, "r", encoding="utf-8") as f:
            wf = json.load(f)

        prefix = f"job_{job_id}"
        wf = deep_replace(wf, {
            "__INPUT_VIDEO__": input_path_for_comfy,
            "__OUTPUT_PREFIX__": prefix,
        })
        t_wf = time.time()

        # 4) Run ComfyUI
        prompt_id = comfy_post_prompt(wf)
        result = wait_until_done(prompt_id, timeout_sec=timeout_sec)
        t_comfy = time.time()

        # 5) Upload output
        out_local = find_latest_output(prefix)
        s3.upload_file(out_local, S3_BUCKET, output_key)
        t_up = time.time()

        # 可选：删输出，避免 /comfyui/output 越跑越大（你也可以注释掉）
        try:
            os.remove(out_local)
        except Exception:
            pass

        return {
            "job_id": job_id,
            "input_key": input_key,
            "output_key": output_key,
            "prompt_id": prompt_id,
            "preprocess": pre_info,
            "timing_sec": {
                "download": round(t_dl - t0, 3),
                "preprocess": round(t_pre - t_dl, 3),
                "workflow_prepare": round(t_wf - t_pre, 3),
                "comfy_run": round(t_comfy - t_wf, 3),
                "upload": round(t_up - t_comfy, 3),
                "total": round(t_up - t0, 3),
            },
            "comfy_status": result.get("status", {}),
        }

    finally:
        shutil.rmtree(workdir, ignore_errors=True)


runpod.serverless.start({"handler": handler})
