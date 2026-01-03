import argparse, os, subprocess

def run(cmd, cwd=None):
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, cwd=cwd)

def main(lock_path: str, dst: str):
    os.makedirs(dst, exist_ok=True)

    with open(lock_path, "r", encoding="utf-8") as f:
        lines = [x.strip() for x in f if x.strip() and not x.strip().startswith("#")]

    for line in lines:
        repo, commit = line.split("|", 1)
        name = repo.rstrip("/").split("/")[-1].replace(".git", "")
        target = os.path.join(dst, name)

        if not os.path.exists(target):
            run(["git", "clone", repo, target, "--no-checkout"])

        run(["git", "fetch", "--all"], cwd=target)
        run(["git", "checkout", commit], cwd=target)

        req = os.path.join(target, "requirements.txt")
        if os.path.exists(req):
            run(["/opt/venv/bin/python", "-m", "pip", "install", "--no-cache-dir", "-r", req])

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lock", required=True)
    ap.add_argument("--dst", required=True)
    args = ap.parse_args()
    main(args.lock, args.dst)

