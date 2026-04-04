from __future__ import annotations

from apps.ml.summarization.prompting import hash_prompt_spec, load_prompt_spec
from tests.helpers import write_summarization_prompt


def test_prompt_loader_reads_required_sections_and_hash_follows_prompt_blocks(tmp_path) -> None:
    prompt_path = write_summarization_prompt(
        tmp_path / "summarization.md",
        system_prompt="Системный промпт v1",
        user_prompt_template="Размер: {size}\n{texts}",
    )

    spec = load_prompt_spec(prompt_path)
    original_hash = hash_prompt_spec(spec)

    updated_prompt_path = write_summarization_prompt(
        tmp_path / "summarization_v2.md",
        system_prompt="Системный промпт v2",
        user_prompt_template="Размер: {size}\n{texts}",
    )
    updated_hash = hash_prompt_spec(load_prompt_spec(updated_prompt_path))

    assert spec.system_prompt == "Системный промпт v1"
    assert spec.user_prompt_template == "Размер: {size}\n{texts}"
    assert original_hash != updated_hash
