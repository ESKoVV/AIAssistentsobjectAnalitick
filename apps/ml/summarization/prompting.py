from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path


HEADING_PATTERN = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)
CODE_BLOCK_PATTERN = re.compile(r"```(?:[A-Za-z0-9_-]+)?\n(.*?)```", re.DOTALL)
WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class PromptSpec:
    task_goal: str
    allowed_input_fields: str
    forbidden_behavior: str
    output_schema: str
    tone_constraints: str
    system_prompt: str
    user_prompt_template: str


def load_prompt_spec(path: str | Path) -> PromptSpec:
    prompt_path = Path(path)
    text = prompt_path.read_text(encoding="utf-8")
    sections = _split_sections(text)

    required_sections = {
        "task goal",
        "allowed input fields",
        "forbidden behavior",
        "output schema",
        "tone constraints",
        "system prompt",
        "user prompt template",
    }
    missing = sorted(required_sections - sections.keys())
    if missing:
        raise ValueError(f"prompt file is missing required sections: {', '.join(missing)}")

    return PromptSpec(
        task_goal=sections["task goal"],
        allowed_input_fields=sections["allowed input fields"],
        forbidden_behavior=sections["forbidden behavior"],
        output_schema=sections["output schema"],
        tone_constraints=sections["tone constraints"],
        system_prompt=_extract_code_block(sections["system prompt"], section_name="System Prompt"),
        user_prompt_template=_extract_code_block(
            sections["user prompt template"],
            section_name="User Prompt Template",
        ),
    )


def hash_prompt_spec(spec: PromptSpec) -> str:
    normalized = "\n".join(
        [
            _normalize_block(spec.system_prompt),
            _normalize_block(spec.user_prompt_template),
        ],
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def render_user_prompt(
    spec: PromptSpec,
    *,
    size: int,
    period_start: str,
    period_end: str,
    source_types: str,
    geo_regions: str,
    texts: str,
    feedback_reason: str | None = None,
) -> str:
    prompt = spec.user_prompt_template.format(
        size=size,
        period_start=period_start,
        period_end=period_end,
        source_types=source_types,
        geo_regions=geo_regions,
        texts=texts,
    ).strip()
    if feedback_reason:
        prompt = (
            f"{prompt}\n\n"
            f"Предыдущий ответ нарушил правило: {feedback_reason}. Перепиши ответ строго по формату."
        )
    return prompt


def _split_sections(text: str) -> dict[str, str]:
    matches = list(HEADING_PATTERN.finditer(text))
    if not matches:
        raise ValueError("prompt file does not contain any level-2 sections")

    sections: dict[str, str] = {}
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        heading = match.group(1).strip().casefold()
        sections[heading] = text[start:end].strip()
    return sections


def _extract_code_block(section_text: str, *, section_name: str) -> str:
    match = CODE_BLOCK_PATTERN.search(section_text)
    if match is None:
        raise ValueError(f"{section_name} section must contain a fenced code block")
    return match.group(1).strip()


def _normalize_block(text: str) -> str:
    lines = [WHITESPACE_PATTERN.sub(" ", line).strip() for line in text.strip().splitlines()]
    return "\n".join(line for line in lines if line)
