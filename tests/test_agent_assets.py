"""Static checks for shared Claude and Codex repository assets."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_SKILL = REPO_ROOT / "skills" / "juvenal"


def test_claude_and_codex_share_root_instructions() -> None:
    agents = REPO_ROOT / "AGENTS.md"
    claude = REPO_ROOT / "CLAUDE.md"

    assert agents.is_symlink()
    assert agents.resolve() == claude.resolve()


def test_claude_and_codex_discover_the_same_skill() -> None:
    claude_skill = REPO_ROOT / ".claude" / "skills" / "juvenal"
    codex_skill = REPO_ROOT / ".agents" / "skills" / "juvenal"

    assert claude_skill.is_symlink()
    assert codex_skill.is_symlink()
    assert claude_skill.resolve() == CANONICAL_SKILL.resolve()
    assert codex_skill.resolve() == CANONICAL_SKILL.resolve()


def test_skill_uses_portable_frontmatter() -> None:
    content = (CANONICAL_SKILL / "SKILL.md").read_text(encoding="utf-8")
    _, frontmatter, _ = content.split("---", maxsplit=2)
    metadata = yaml.safe_load(frontmatter)

    assert set(metadata) == {"name", "description"}
    assert metadata["name"] == "juvenal"
    assert "Claude" in metadata["description"]
    assert "Codex" in metadata["description"]


def test_packaged_plugin_skill_matches_canonical_skill() -> None:
    canonical = (CANONICAL_SKILL / "SKILL.md").read_bytes()
    packaged = (REPO_ROOT / "plugin" / "skills" / "juvenal" / "SKILL.md").read_bytes()

    assert packaged == canonical
