"""bootstrap.sh on an Azure workspace: the Azure installer, with no AWS tool, value or call.

The one command is the same on both clouds (`./bootstrap.sh --apply --deploy-app --yes`), and an
`azuredatabricks.net` host hands the run to `./antidemo azure-setup` before anything AWS's runs.
Copies of bootstrap.sh run here against stubs: `aws` is absent from PATH, and `./antidemo` only
records how it was called.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from test_bootstrap_stub_harness import SYSTEM_PATH_DIRS, _sanitised_system_path

REPO = Path(__file__).resolve().parent.parent
AZURE_TOOLS = ("uv", "node", "npm", "databricks", "az", "terraform", "python3", "jq")
ENV = (
    "DATABRICKS_HOST=https://example.azuredatabricks.net\n"
    "DATABRICKS_PROFILE=anti-demo-azure-test\n"
    "AZURE_TENANT_ID=aaaaaaaa-0000-0000-0000-000000000000\n"
    "AZURE_SUBSCRIPTION_ID=00000000-1111-2222-3333-444444444444\n"
    "AZURE_RESOURCE_GROUP=rg-test\n"
    "AZURE_REGION=westus2\n"
)


def run(tmp_path: Path, env_text: str, *, ambient: dict[str, str] | None = None):
    tree, bin_dir = tmp_path / "tree", tmp_path / "bin"
    (tree / ".venv" / "bin").mkdir(parents=True)
    bin_dir.mkdir()
    shutil.copy2(REPO / "bootstrap.sh", tree / "bootstrap.sh")
    record = tmp_path / "antidemo-calls"
    launcher = tree / "antidemo"
    launcher.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s\\n' \"$*\" >> {record}\n"
        f"env | sort > {tmp_path / 'antidemo-env'}\n", encoding="utf-8")
    launcher.chmod(0o755)
    python = tree / ".venv" / "bin" / "python"
    python.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    python.chmod(0o755)
    for tool in AZURE_TOOLS:
        shim = bin_dir / tool
        if tool == "python3":
            # The real interpreter: bootstrap reads its inputs with it before the dispatch.
            shim.symlink_to(shutil.which("python3") or "/usr/bin/python3")
            continue
        shim.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        shim.chmod(0o755)
    path = f"{bin_dir}:{_sanitised_system_path(tmp_path, SYSTEM_PATH_DIRS)}"
    assert shutil.which("aws", path=path) is None, "aws must be absent for these runs"
    (tmp_path / "env").write_text(env_text, encoding="utf-8")
    result = subprocess.run(
        [shutil.which("bash") or "bash", str(tree / "bootstrap.sh"), "--apply", "--deploy-app",
         "--yes", "--env-file", str(tmp_path / "env")],
        cwd=tree, env={"PATH": path, "HOME": str(tmp_path), **(ambient or {})},
        stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120)
    calls = record.read_text(encoding="utf-8").splitlines() if record.exists() else []
    handed = (tmp_path / "antidemo-env").read_text() if (tmp_path / "antidemo-env").exists() else ""
    return result, calls, handed


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_an_azure_workspace_is_handed_to_the_azure_installer_with_no_aws_tool(tmp_path):
    result, calls, handed = run(tmp_path, ENV, ambient={"AWS_PROFILE": "someone-elses"})
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert calls == ["azure-setup"]
    assert "installing the Azure edition" in output
    # An ambient AWS setting isn't handed on, and the Azure settings are.
    assert "AWS_PROFILE=" not in handed
    assert "AZURE_SUBSCRIPTION_ID=00000000-1111-2222-3333-444444444444" in handed


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is not on PATH")
def test_an_aws_value_in_an_azure_env_file_is_named_and_refused(tmp_path):
    result, calls, _ = run(tmp_path, ENV + "AWS_ACCESS_KEY_ID=AKIASTUB\n")
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "remove AWS_ACCESS_KEY_ID" in output
    assert calls == []
