from dataclasses import replace

from podcast_frequency_list.tokens import (
    SpanGenerationError,
    generate_sentence_spans,
    tokenize_sentence_text,
)


def _spans_for(text: str, *, max_ngram_size: int = 3):
    return generate_sentence_spans(
        sentence_id=10,
        episode_id=20,
        segment_id=30,
        sentence_text=text,
        tokens=tokenize_sentence_text(text),
        max_ngram_size=max_ngram_size,
    )


def test_generate_sentence_spans_preserves_apostrophe_surface_text() -> None:
    spans = _spans_for("J'ai envie de dire.")
    spans_by_key = {span.candidate_key: span for span in spans}

    assert "j" not in spans_by_key
    assert spans_by_key["j ai"].surface_text == "J'ai"
    assert spans_by_key["j ai"].display_text == "J'ai"
    assert spans_by_key["j ai"].ngram_size == 2
    assert spans_by_key["j ai"].token_start_index == 0
    assert spans_by_key["j ai"].token_end_index == 2
    assert spans_by_key["j ai envie"].surface_text == "J'ai envie"


def test_generate_sentence_spans_preserves_elision_surface_text() -> None:
    spans = _spans_for("L'homme est là.")
    spans_by_key = {span.candidate_key: span for span in spans}

    assert "l" not in spans_by_key
    assert spans_by_key["l homme"].surface_text == "L'homme"
    assert spans_by_key["l homme est"].surface_text == "L'homme est"


def test_generate_sentence_spans_filters_standalone_clitics_but_keeps_chunks() -> None:
    spans = _spans_for("J'ai dit qu'il arrive.")
    keys = {span.candidate_key for span in spans}

    assert "j" not in keys
    assert "qu" in keys
    assert "il" in keys
    assert "j ai" in keys
    assert "qu il" in keys


def test_generate_sentence_spans_filters_numeric_only_spans() -> None:
    spans = _spans_for("22 fois et 1-0.")
    keys = {span.candidate_key for span in spans}

    assert "22" not in keys
    assert "1-0" not in keys
    assert "22 fois" in keys
    assert "fois et 1-0" in keys


def test_generate_sentence_spans_respects_max_ngram_size() -> None:
    spans = _spans_for("Je pense que oui.", max_ngram_size=2)

    assert {span.ngram_size for span in spans} == {1, 2}
    assert "je pense que" not in {span.candidate_key for span in spans}


def test_generate_sentence_spans_sorts_valid_tokens_by_token_index() -> None:
    text = "Je pense."
    tokens = tuple(reversed(tokenize_sentence_text(text)))

    spans = generate_sentence_spans(
        sentence_id=10,
        episode_id=20,
        segment_id=30,
        sentence_text=text,
        tokens=tokens,
    )

    assert [span.candidate_key for span in spans] == ["je", "je pense", "pense"]


def test_generate_sentence_spans_rejects_bad_token_offsets() -> None:
    text = "Je pense."
    token = tokenize_sentence_text(text)[0]
    bad_token = replace(token, surface_text="Tu")

    try:
        generate_sentence_spans(
            sentence_id=10,
            episode_id=20,
            segment_id=30,
            sentence_text=text,
            tokens=(bad_token,),
        )
    except SpanGenerationError as exc:
        assert "surface" in str(exc)
    else:
        raise AssertionError("expected SpanGenerationError")


def test_generate_sentence_spans_rejects_noncontiguous_token_indexes() -> None:
    text = "Je pense."
    first, second = tokenize_sentence_text(text)
    bad_second = replace(second, token_index=2)

    try:
        generate_sentence_spans(
            sentence_id=10,
            episode_id=20,
            segment_id=30,
            sentence_text=text,
            tokens=(first, bad_second),
        )
    except SpanGenerationError as exc:
        assert "contiguous" in str(exc)
    else:
        raise AssertionError("expected SpanGenerationError")


def test_generate_sentence_spans_rejects_invalid_max_ngram_size() -> None:
    try:
        _spans_for("Je pense.", max_ngram_size=0)
    except SpanGenerationError as exc:
        assert "max_ngram_size" in str(exc)
    else:
        raise AssertionError("expected SpanGenerationError")
