"""Run existing experiments in an isolated notebook result directory."""
import sys
import runpy
import uuid
import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "experiments"))

def main():
    import common
    model = sys.argv[1]
    if model not in {"ryw", "mr", "mw", "wfr"}:
        raise SystemExit("Unknown model")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex
    output = ROOT / "results" / "notebook_runs" / run_id
    output.mkdir(parents=True)
    common.RESULTS_DIR = str(output)
    (output / "manifest.json").write_text(json.dumps({
        "run_id": run_id, "model": model, "arguments": sys.argv[2:],
        "python": sys.executable,
        "scope": "Existing experiment logic; isolated output and UUID run identifiers."
    }, ensure_ascii=False, indent=2))
    # Replace only the legacy run-id formatter. Other time formatting is unchanged.
    original = common.time.strftime
    common.time.strftime = lambda fmt, *args: run_id if fmt == "%H%M%S" else original(fmt, *args)
    sys.argv = [str(ROOT / "experiments" / f"{model}_test.py"), *sys.argv[2:]]
    print("Notebook run:", run_id, flush=True)
    print("Results:", output, flush=True)
    try:
        runpy.run_path(sys.argv[0], run_name="__main__")
    finally:
        common.time.strftime = original

if __name__ == "__main__":
    main()

