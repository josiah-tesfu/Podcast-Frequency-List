from pathlib import Path

import pytest

from podcast_frequency_list.config import PROJECT_ROOT
from podcast_frequency_list.show_manifest import (
    DEFAULT_SHOW_MANIFEST_PATH,
    ShowManifestError,
    load_show_manifest,
)


def test_load_show_manifest_defaults_to_repo_csv() -> None:
    assert DEFAULT_SHOW_MANIFEST_PATH == PROJECT_ROOT / "data/show_manifest.csv"
    assert DEFAULT_SHOW_MANIFEST_PATH.exists()


def test_show_manifest_contains_expected_show_rows() -> None:
    rows = load_show_manifest()

    assert len(rows) == 10
    assert all(row.enabled for row in rows)
    assert all(row.selection_order == "newest" for row in rows)
    assert all(row.target_hours == 10 for row in rows)
    assert {row.slug for row in rows} == {
        "zack-en-roue-libre",
        "small-talk-konbini",
        "un-bon-moment",
        "a-bientot-de-te-revoir",
        "les-gens-qui-doutent",
        "floodcast",
        "legend",
        "tftc-le-podcast",
        "contre-soiree",
        "la-lecon",
    }


def test_load_show_manifest_rejects_duplicate_slug(tmp_path: Path) -> None:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "show-a,Show A,https://example.com/a.xml,fr,native,test,10,newest,1,one",
                "show-a,Show B,https://example.com/b.xml,fr,native,test,10,newest,1,two",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ShowManifestError, match="duplicate slug"):
        load_show_manifest(manifest_path)


def test_load_show_manifest_rejects_duplicate_feed_url(tmp_path: Path) -> None:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "show-a,Show A,https://example.com/shared.xml,fr,native,test,10,newest,1,one",
                "show-b,Show B,https://example.com/shared.xml,fr,native,test,10,newest,1,two",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ShowManifestError, match="duplicate feed_url"):
        load_show_manifest(manifest_path)


def test_load_show_manifest_requires_positive_target_hours_for_enabled_rows(tmp_path: Path) -> None:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "show-a,Show A,https://example.com/a.xml,fr,native,test,0,newest,1,one",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ShowManifestError, match="target_hours > 0"):
        load_show_manifest(manifest_path)


def test_load_show_manifest_requires_newest_selection_order(tmp_path: Path) -> None:
    manifest_path = tmp_path / "show_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "slug,title,feed_url,language,bucket,family,target_hours,selection_order,enabled,notes",
                "show-a,Show A,https://example.com/a.xml,fr,native,test,10,oldest,1,one",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ShowManifestError, match="selection_order must be newest"):
        load_show_manifest(manifest_path)
