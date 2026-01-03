import os, json, time, shutil, uuid, subprocess, glob
import requests
import boto3
import runpod

COMFY_PORT = os.getenv("COMFYUI_PORT", "8188")
COMFY = f"http://127.0.0.1:{COMFY_PORT}"

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")

if not all([S3_ENDPOINT_URL, S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY]):
    raise RuntimeError("Missing S3_* env vars (R2/S3 credentials).")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    region_name="auto",
)

def deep_replace(obj, mapping):
    if isinstance(obj, dict):
        return {k: deep_replace(v, mapping) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_replace(x, mapping) for x in obj]
    if isinstance(obj, str):
        for a, b in mapping.items():
            obj = obj.replace(a, b)
        return obj
    return obj

def comfy_post(path, payload):
    r = requests.post(f"{COMFY}{path}", json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def comfy_get(path):
    r = requests.get(f"{COMFY}{path}", timeout=60)
    r.raise_for_status()
    return r.json()

def run_workflow(workflow: dict) -> str:
    out = comfy_post("/prompt", {"prompt": workflow, "client_id": str(uuid.uuid4())})
    prompt_id = out["prompt_id"]

    # 等完成：/history/<prompt_id> 出现该 id 就表示 finished
    for _ in range(60 * 60):  # up to 1 hour
        hist = comfy_get(f"/history/{prompt_id}")
        if prompt_id in hist:
            return prompt_id
        time.sleep(1)

    raise TimeoutError("ComfyUI job timeout")

def find_latest_output(prefix: str) -> str:
    out_dir = "/comfyui/output"
    cand = sorted(
        glob.glob(os.path.join(out_dir, f"{prefix}*")),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )
    # 优先拿 mp4
    mp4 = [p for p in cand if p.lower().endswith(".mp4")]
    if mp4:
        return mp4[0]
    if cand:
        return cand[0]
    raise FileNotFoundError("No output file found in /comfyui/output")

def fps_downsample(src, dst, fps):
    if not fps:
        shutil.copy2(src, dst)
        return dst
    fps = int(fps)
    cmd = ["ffmpeg", "-y", "-i", src, "-vf", f"fps={fps}", "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", dst]
    subprocess.check_call(cmd)
    return dst

def handler(job):
    inp = job.get("input", {}) or {}
    job_id = inp.get("job_id") or job.get("id") or str(uuid.uuid4())

    input_key = inp["input_key"]
    output_key = inp["output_key"]
    params = inp.get("params", {}) or {}

    workdir = f"/tmp/jobs/{job_id}"
    os.makedirs(workdir, exist_ok=True)

    local_in = os.path.join(workdir, "in.mp4")
    local_in2 = os.path.join(workdir, "in_fps.mp4")
    prefix = f"job_{job_id}"

    try:
        # 1) R2 下载到本地临时目录
        s3.download_file(S3_BUCKET, input_key, local_in)

        # 2) 可选：先降帧（减少后面工作流压力）
        fps_limit = params.get("fps_limit")
        fps_downsample(local_in, local_in2, fps_limit)

        # 3) 读取 workflow 模板，替换占位符
        with open("/comfyui/workflows/workflow_api.json", "r", encoding="utf-8") as f:
            wf = json.load(f)

        wf = deep_replace(wf, {
            "__INPUT_VIDEO__": local_in2,
            "__OUTPUT_PREFIX__": prefix,
        })

        # 4) 跑 ComfyUI
        run_workflow(wf)

        # 5) 找输出文件并上传回 R2
        out_file = find_latest_output(prefix)
        s3.upload_file(out_file, S3_BUCKET, output_key)

        return {
            "job_id": job_id,
            "status": "ok",
            "input_key": input_key,
            "output_key": output_key,
        }

    finally:
        # 6) 清理临时文件，避免磁盘越跑越满
        shutil.rmtree(workdir, ignore_errors=True)

runpod.serverless.start({"handler": handler})
