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
COMFY_PORT = int(os.environ.get("COMFY_PORT", "8188"))
COMFY_BASE = f"http://{COMFY_HOST}:{COMFY_PORT}"

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")

# Default preprocessing policy
DEFAULT_FPS_CAP = 24
DEFAULT_MAX_SECONDS = 15

# Where workflow template lives inside the image
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
    """
    Returns (fps, duration_seconds) from ffprobe.
    fps may be None if cannot be determined.
    duration may be None if cannot be determined.
    """
    # ffprobe JSON
    out = sh([
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=avg_frame_rate,r_frame_rate,duration",
        "-of", "json",
        path
    ])
    try:
        data = json.loads(out)
        stream = (data.get("streams") or [{}])[0]
        afr = stream.get("avg_frame_rate") or "0/0"
        rfr = stream.get("r_frame_rate") or "0/0"

        fps = None
        for frac in (afr, rfr):
            try:
                f = Fraction(frac)
                if f.numerator != 0 and f.denominator != 0:
                    fps = float(f)
                    break
            except Exception:
                pass

        dur = None
        d = stream.get("duration")
        if d is not None:
            try:
                dur = float(d)
            except Exception:
                dur = None

        return fps, dur
    except Exception:
        return None, None


def preprocess_video(
    src: str,
    dst: str,
    fps_cap: int,
    max_seconds: int
) -> Dict[str, Any]:
    """
    - If fps > fps_cap, downsample to fps_cap.
    - If duration > max_seconds, trim to first max_seconds seconds.
    - If no change needed, reuse src (no transcode).
    Returns a dict with info and sets 'path' to the chosen file.
    """
    fps_in, dur_in = probe_video(src)

    # Decide whether we must transcode
    need_fps = (fps_in is not None and fps_cap > 0 and fps_in > (fps_cap + 0.01))
    # Even if duration cannot be probed, we still enforce -t for safety (protect compute)
    need_trim = (max_seconds > 0) and (dur_in is None or (dur_in > (max_seconds + 0.001)))

    # If nothing to do, just use src
    if not need_fps and not need_trim:
        return {
            "path": src,
            "fps_in": fps_in,
            "dur_in": dur_in,
            "fps_cap": fps_cap,
            "max_seconds": max_seconds,
            "downsampled": False,
            "trimmed": False,
            "transcoded": False,
        }

    vf = []
    if need_fps:
        vf.append(f"fps={int(fps_cap)}")

    cmd = ["ffmpeg", "-y", "-i", src]
    # Trim first (protect compute); applies to audio+video
    if max_seconds > 0:
        cmd += ["-t", str(int(max_seconds))]
    if vf:
        cmd += ["-vf", ",".join(vf)]

    # Keep audio (your workflow later combines audio)
    cmd += [
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "18",
        "-c:a", "aac",
        "-b:a", "192k",
        "-movflags", "+faststart",
        dst
    ]

    # Run ffmpeg
    _ = sh(cmd, check=True)

    fps_out, dur_out = probe_video(dst)

    return {
        "path": dst,
        "fps_in": fps_in,
        "dur_in": dur_in,
        "fps_out": fps_out,
        "dur_out": dur_out,
        "fps_cap": fps_cap,
        "max_seconds": max_seconds,
        "downsampled": bool(need_fps),
        "trimmed": bool(need_trim),
        "transcoded": True,
    }


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
            # ComfyUI usually marks "completed"
            if status.get("completed", False) or status.get("status_str") == "success":
                return hist[prompt_id]
            if status.get("status_str") == "error":
                raise RuntimeError(f"ComfyUI error: {json.dumps(status, ensure_ascii=False)}")
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(f"ComfyUI job timeout after {timeout_sec}s, prompt_id={prompt_id}")
        time.sleep(poll)


def find_latest_output(prefix: str) -> str:
    """
    Find latest mp4 in /comfyui/output whose filename starts with prefix.
    """
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

    # output_key optional
    job_id = inp.get("job_id") or uuid.uuid4().hex[:12]
    output_key = inp.get("output_key") or f"outputs/{job_id}.mp4"

    params = inp.get("params") or {}

    # New policy:
    # - cap fps at min(original fps, cap) where cap = params.fps_limit if provided else 24
    # - max_seconds default 15
    fps_cap = params.get("fps_limit")
    try:
        fps_cap = int(fps_cap) if fps_cap is not None else DEFAULT_FPS_CAP
    except Exception:
        fps_cap = DEFAULT_FPS_CAP

    max_seconds = params.get("max_seconds")
    try:
        max_seconds = int(max_seconds) if max_seconds is not None else DEFAULT_MAX_SECONDS
    except Exception:
        max_seconds = DEFAULT_MAX_SECONDS

    # Working dir
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

    # 1) Download input from R2
    s3.download_file(S3_BUCKET, input_key, local_in)

    # 2) Preprocess: cap fps + trim to max_seconds
    pre_info = preprocess_video(local_in, local_pre, fps_cap=fps_cap, max_seconds=max_seconds)
    input_path_for_comfy = pre_info["path"]

    # 3) Load workflow template and replace placeholders
    with open(WORKFLOW_PATH, "r", encoding="utf-8") as f:
        wf = json.load(f)

    prefix = f"job_{job_id}"
    wf = deep_replace(wf, {
        "__INPUT_VIDEO__": input_path_for_comfy,
        "__OUTPUT_PREFIX__": prefix,
    })

    # 4) Submit to ComfyUI and wait
    prompt_id = comfy_post_prompt(wf)
    result = wait_until_done(prompt_id, timeout_sec=int(params.get("timeout_sec", 3600)))

    # 5) Find output and upload to R2
    out_local = find_latest_output(prefix)
    s3.upload_file(out_local, S3_BUCKET, output_key)

    # Optional cleanup
    try:
        shutil.rmtree(workdir, ignore_errors=True)
    except Exception:
        pass

    return {
        "job_id": job_id,
        "input_key": input_key,
        "output_key": output_key,
        "prompt_id": prompt_id,
        "preprocess": pre_info,
        "comfy_result_status": result.get("status", {}),
    }


runpod.serverless.start({"handler": handler})
