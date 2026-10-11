import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _copy_repo(dst: Path) -> None:
    for p in ROOT.glob("*.py"):
        shutil.copy(p, dst / p.name)
    (dst / "term_dictionary").mkdir()
    for p in (ROOT / "term_dictionary").glob("*.py"):
        shutil.copy(p, dst / "term_dictionary" / p.name)


def _run(code: str, cwd: Path, env_extra=None) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("MYSQL_")}
    env.update(env_extra or {})
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=str(cwd), env=env,
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip().splitlines()[-1]


def test_dotenv_is_loaded_before_modules_read_settings(tmp_path):
    repo = tmp_path / "rag-preparer"
    repo.mkdir()
    _copy_repo(repo)
    (repo / ".env").write_text(
        "MYSQL_HOST=envfile-host\nMYSQL_PASSWORD=envfile-pw\nRAG_PREPARER_MAX_ATTEMPT=7\n",
        encoding="utf-8",
    )
    elsewhere = tmp_path / "elsewhere"  # 다른 작업 디렉터리에서 실행해도 스크립트 옆 .env를 읽는지
    elsewhere.mkdir()

    code = (
        f"import sys; sys.path.insert(0, {str(repo)!r}); "
        "import run_pipeline, candidate_queue, enrich_case_metadata, pipeline_state; "
        "c = pipeline_state.DBConfig.from_env(); "
        "print(candidate_queue.MYSQL_HOST, enrich_case_metadata.MYSQL_PASS, "
        "c.host, c.password, run_pipeline.MAX_ATTEMPT)"
    )
    assert _run(code, elsewhere) == "envfile-host envfile-pw envfile-host envfile-pw 7"

    # 이미 설정된 환경변수가 .env보다 우선
    assert _run(code, elsewhere, {"MYSQL_HOST": "real-env"}).startswith("real-env ")

    for script, attr in (("promote_candidate_terms", "MYSQL_HOST"), ("upload_term_index", "MYSQL_PASS")):
        code = (
            f"import sys; sys.path.insert(0, {str(repo / 'term_dictionary')!r}); "
            f"import {script} as m; print(m.{attr})"
        )
        expected = "envfile-host" if attr == "MYSQL_HOST" else "envfile-pw"
        assert _run(code, elsewhere) == expected
