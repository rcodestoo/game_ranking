"""
AGS Game Ranking Tool — Test Suite
===================================
Tests for scoring logic, date normalisation, upload normalisation,
and the pipeline merge/append behaviour.

Run with:
    cd game_ranking && python -m pytest tests/test_suite.py -v
"""

import io
import sys
import os
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# Make game_ranking importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _csv_bytes(df: pd.DataFrame, encoding: str = "utf-8") -> bytes:
    return df.to_csv(index=False).encode(encoding)


# ══════════════════════════════════════════════════════════════════════════════
# 1. SCORING FUNCTIONS  (calculation/process_data.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestCalculateHybridScore:
    """calculate_hybrid_score — hybrid linear/log normalisation to 1–5."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from calculation.process_data import calculate_hybrid_score
        self.fn = calculate_hybrid_score

    def test_min_value_returns_1(self):
        assert self.fn(1000, 1000, 100_000) == pytest.approx(1.0)

    def test_max_value_returns_5(self):
        assert self.fn(100_000, 1000, 100_000) == pytest.approx(5.0)

    def test_midpoint_is_between_1_and_5(self):
        score = self.fn(50_000, 1000, 100_000)
        assert 1.0 < score < 5.0

    def test_higher_value_gives_higher_score(self):
        low  = self.fn(5_000,  1000, 100_000)
        high = self.fn(80_000, 1000, 100_000)
        assert high > low

    def test_output_range_never_exceeded(self):
        for v in [1000, 10_000, 50_000, 99_999, 100_000]:
            s = self.fn(v, 1000, 100_000)
            assert 1.0 <= s <= 5.0


class TestCalculateTrendsWeightedPoints:
    """calculate_trends_weighted_points — 0–100 → 1–5 linear."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from calculation.process_data import calculate_trends_weighted_points
        self.fn = calculate_trends_weighted_points

    def test_zero_trends_score_returns_1(self):
        assert self.fn(0) == pytest.approx(1.0)

    def test_100_trends_score_returns_5(self):
        assert self.fn(100) == pytest.approx(5.0)

    def test_50_trends_score_returns_3(self):
        assert self.fn(50) == pytest.approx(3.0)

    def test_monotonically_increasing(self):
        scores = [self.fn(v) for v in range(0, 101, 10)]
        assert scores == sorted(scores)


class TestCleanDevGenreList:
    """clean_dev_genre_list — splits comma-separated strings into lists."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from calculation.process_data import clean_dev_genre_list
        self.fn = clean_dev_genre_list

    def test_splits_developers(self):
        df = pd.DataFrame({"Developers": ["Dev A, Dev B"], "Genres": ["Action"]})
        result = self.fn(df)
        assert result["Developers"].iloc[0] == ["Dev A", " Dev B"]

    def test_splits_genres(self):
        df = pd.DataFrame({"Developers": ["Dev A"], "Genres": ["Action,RPG,Indie"]})
        result = self.fn(df)
        assert len(result["Genres"].iloc[0]) == 3

    def test_preserves_inc_suffix(self):
        df = pd.DataFrame({"Developers": ["Acme, Inc., Other"], "Genres": ["Action"]})
        result = self.fn(df)
        # "Inc." suffix should be joined so it doesn't produce a spurious split
        devs = result["Developers"].iloc[0]
        assert any("Inc." in d for d in devs)

    def test_single_developer_is_list(self):
        df = pd.DataFrame({"Developers": ["Solo Dev"], "Genres": ["Action"]})
        result = self.fn(df)
        assert isinstance(result["Developers"].iloc[0], list)


class TestFlagging:
    """flagging — computes Is_Indie, Has_Multiple_Developers, Has_Multiple_Genres."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from calculation.process_data import clean_dev_genre_list, flagging
        self.flagging = flagging
        self.clean = clean_dev_genre_list

    def _make_df(self, name, devs, genres, followers=1000):
        df = pd.DataFrame({
            "Name": [name],
            "ReleaseDate": ["2025-01-01"],
            "Developers": [devs],
            "Genres": [genres],
            "FollowerCount": [followers],
        })
        return self.clean(df)

    def test_indie_flag_set(self):
        df = self._make_df("Game", "Dev A", "Indie,Action")
        result = self.flagging(df)
        assert result["Is_Indie"].iloc[0] is True

    def test_indie_flag_not_set(self):
        df = self._make_df("Game", "Dev A", "Action,RPG")
        result = self.flagging(df)
        assert result["Is_Indie"].iloc[0] is False

    def test_multiple_developers(self):
        df = self._make_df("Game", "Dev A, Dev B", "Action")
        result = self.flagging(df)
        assert result["Has_Multiple_Developers"].iloc[0] is True

    def test_single_developer(self):
        df = self._make_df("Game", "Solo Dev", "Action")
        result = self.flagging(df)
        assert result["Has_Multiple_Developers"].iloc[0] is False

    def test_multiple_genres(self):
        df = self._make_df("Game", "Dev A", "Action,RPG")
        result = self.flagging(df)
        assert result["Has_Multiple_Genres"].iloc[0] is True


# ══════════════════════════════════════════════════════════════════════════════
# 2. NORMALISER  (pipelines/normalizer.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestReadCsvAutoEncoding:
    """read_csv_auto_encoding — tries multiple encodings."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.normalizer import read_csv_auto_encoding
        self.fn = read_csv_auto_encoding

    def test_reads_utf8(self):
        df = pd.DataFrame({"A": [1, 2]})
        result_df, enc = self.fn(_csv_bytes(df, "utf-8"))
        assert enc == "utf-8"
        assert list(result_df["A"]) == [1, 2]

    def test_reads_latin1(self):
        df = pd.DataFrame({"Name": ["Café"]})
        result_df, enc = self.fn(_csv_bytes(df, "latin-1"))
        assert enc in ("latin-1", "cp1252", "utf-8-sig", "utf-8")

    def test_encoding_detected_correctly_for_utf8(self):
        # utf-8 should be detected on the first try
        df = pd.DataFrame({"Name": ["Test Game"]})
        _, enc = self.fn(_csv_bytes(df, "utf-8"))
        assert enc == "utf-8"


class TestNormalizeSteamReleaseDates:
    """_normalize_steam_release_dates — converts various date formats to dd-mm-yyyy."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.normalizer import _normalize_steam_release_dates
        self.fn = _normalize_steam_release_dates

    def _run(self, values):
        series, _ = self.fn(pd.Series(values))
        return series.tolist()

    def test_dd_mon_yyyy(self):
        result = self._run(["22 Apr, 2026"])
        assert result[0] == "22-04-2026"

    def test_mon_dd_yyyy(self):
        result = self._run(["Apr 23, 2026"])
        assert result[0] == "23-04-2026"

    def test_month_yyyy_maps_to_first(self):
        result = self._run(["April 2026"])
        assert result[0] == "01-04-2026"

    def test_iso_format_passthrough(self):
        result = self._run(["2026-04-15"])
        assert result[0] == "15-04-2026"

    def test_coming_soon_kept_as_is(self):
        result = self._run(["Coming Soon"])
        assert result[0] == "Coming Soon"

    def test_tbd_kept_as_is(self):
        result = self._run(["TBD"])
        assert result[0] == "TBD"

    def test_none_passthrough(self):
        result = self._run([None])
        assert result[0] is None or pd.isna(result[0])

    def test_changed_count_correct(self):
        series = pd.Series(["22 Apr, 2026", "Already Fine"])
        _, changed = self.fn(series)
        assert changed >= 1


class TestPrepareSteamUpload:
    """prepare_steam_upload — end-to-end normalisation of a Steam CSV upload."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.normalizer import prepare_steam_upload
        self.fn = prepare_steam_upload

    def _make_steam_bytes(self, extra_cols=None):
        data = {
            "AppId": [12345, 67890],
            "Name": ["Game Alpha", "Game Beta"],
            "FollowerCount": [5000, 12000],
            "Genres": ["Action", "RPG"],
            "Developers": ["Dev A", "Dev B"],
            "ReleaseDate": ["22 Apr, 2026", "2026-01-15"],
        }
        if extra_cols:
            data.update(extra_cols)
        return _csv_bytes(pd.DataFrame(data))

    def test_returns_dataframe_and_warnings(self):
        df, warnings = self.fn(self._make_steam_bytes())
        assert isinstance(df, pd.DataFrame)
        assert isinstance(warnings, list)

    def test_appid_lowercase_renamed_to_appid(self):
        # Gawk-3000 sometimes exports "appid" (lowercase) — should be renamed to "AppId"
        raw = pd.DataFrame({
            "appid": [1], "Name": ["G"], "FollowerCount": [100],
            "Genres": ["Action"], "Developers": ["D"], "ReleaseDate": ["2026-01-01"],
        })
        df, _ = self.fn(_csv_bytes(raw))
        assert "AppId" in df.columns
        assert "appid" not in df.columns

    def test_release_date_normalised(self):
        df, _ = self.fn(self._make_steam_bytes())
        assert df["ReleaseDate"].iloc[0] == "22-04-2026"

    def test_row_count_preserved(self):
        df, _ = self.fn(self._make_steam_bytes())
        assert len(df) == 2


class TestPrepareNonSteamUpload:
    """prepare_nonsteam_upload — minimal normalisation (encoding only)."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.normalizer import prepare_nonsteam_upload
        self.fn = prepare_nonsteam_upload

    def _make_ns_bytes(self):
        df = pd.DataFrame({
            "Game Title": ["Indie X", "Console Y"],
            "Developers": ["Dev A", "Dev B"],
            "SteamStatus": ["Non-Steam PC Game", "Console Only"],
            "YouTube Views": [10000, 5000],
        })
        return _csv_bytes(df)

    def test_returns_dataframe_and_warnings(self):
        df, warnings = self.fn(self._make_ns_bytes())
        assert isinstance(df, pd.DataFrame)
        assert isinstance(warnings, list)

    def test_row_count_preserved(self):
        df, _ = self.fn(self._make_ns_bytes())
        assert len(df) == 2


# ══════════════════════════════════════════════════════════════════════════════
# 3. NONSTEAM PIPELINE — date normalisation  (pipelines/nonsteam_pipeline.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestNormalizeReleaseDate:
    """_normalize_release_date — mixed slash formats → YYYY-MM-DD."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.nonsteam_pipeline import _normalize_release_date
        self.fn = _normalize_release_date

    def _run(self, values):
        return self.fn(pd.Series(values)).tolist()

    def test_iso_format(self):
        assert self._run(["2026-04-15"])[0] == "2026-04-15"

    def test_day_first_unambiguous(self):
        # day=15 > 12, so must be D/M/YYYY
        assert self._run(["15/1/2026"])[0] == "2026-01-15"

    def test_month_first_unambiguous(self):
        # month=3, day=17 > 12 → M/D/YYYY
        assert self._run(["3/17/2026"])[0] == "2026-03-17"

    def test_ambiguous_defaults_to_day_first(self):
        # 5/6/2026 → ambiguous → D/M → 2026-06-05
        assert self._run(["5/6/2026"])[0] == "2026-06-05"

    def test_coming_soon_kept(self):
        assert self._run(["Coming Soon"])[0] == "Coming Soon"

    def test_tbd_kept(self):
        assert self._run(["TBD"])[0] == "TBD"

    def test_none_returns_none(self):
        result = self._run([None])[0]
        assert result is None or pd.isna(result)

    def test_empty_string_returns_none(self):
        result = self._run([""])[0]
        assert result is None or pd.isna(result)


class TestNormalizeNonSteamDf:
    """_normalize_nonsteam_df — enforces canonical column set."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.nonsteam_pipeline import _normalize_nonsteam_df, NONSTEAM_COLUMNS
        self.fn = _normalize_nonsteam_df
        self.cols = NONSTEAM_COLUMNS

    def test_output_has_all_canonical_columns(self):
        df = pd.DataFrame({"Game Title": ["X"], "Developers": ["D"]})
        result = self.fn(df)
        assert list(result.columns) == self.cols

    def test_alias_name_renamed_to_game_title(self):
        df = pd.DataFrame({"Name": ["X"]})
        result = self.fn(df)
        assert "Game Title" in result.columns
        assert result["Game Title"].iloc[0] == "X"

    def test_missing_columns_filled_with_none(self):
        df = pd.DataFrame({"Game Title": ["X"]})
        result = self.fn(df)
        assert result["Developers"].iloc[0] is None


# ══════════════════════════════════════════════════════════════════════════════
# 4. STEAM PIPELINE MERGE  (pipelines/steam_pipeline.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestAppendFromUploadedSteamCsv:
    """append_from_uploaded_steam_csv — merge logic."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.steam_pipeline import append_from_uploaded_steam_csv
        self.fn = append_from_uploaded_steam_csv

    def _make_steam_df(self, app_ids, names=None):
        names = names or [f"Game {i}" for i in app_ids]
        return pd.DataFrame({
            "AppId": app_ids,
            "Name": names,
            "FollowerCount": [1000] * len(app_ids),
            "Genres": ["Action"] * len(app_ids),
            "Developers": ["Dev A"] * len(app_ids),
            "ReleaseDate": ["2026-01-01"] * len(app_ids),
        })

    def test_all_new_rows_appended(self, tmp_path):
        existing = self._make_steam_df([1, 2])
        upload   = self._make_steam_df([3, 4])

        existing_csv = tmp_path / "raw_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.steam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.steam_pipeline.get_latest_steam_csv", return_value=existing_csv):
            n_updated, n_new = self.fn(upload)

        assert n_new == 2
        assert n_updated == 0
        out = pd.read_csv(tmp_path / f"raw_steam_{date.today()}.csv")
        assert len(out) == 4

    def test_existing_rows_overwritten(self, tmp_path):
        existing = self._make_steam_df([1, 2], names=["Old Name 1", "Old Name 2"])
        upload   = self._make_steam_df([1], names=["New Name 1"])

        existing_csv = tmp_path / "raw_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.steam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.steam_pipeline.get_latest_steam_csv", return_value=existing_csv):
            n_updated, n_new = self.fn(upload)

        assert n_updated == 1
        assert n_new == 0
        out = pd.read_csv(tmp_path / f"raw_steam_{date.today()}.csv")
        assert len(out) == 2
        updated_row = out[out["AppId"] == 1]
        assert updated_row["Name"].iloc[0] == "New Name 1"

    def test_untouched_rows_preserved(self, tmp_path):
        existing = self._make_steam_df([1, 2, 3])
        upload   = self._make_steam_df([1])

        existing_csv = tmp_path / "raw_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.steam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.steam_pipeline.get_latest_steam_csv", return_value=existing_csv):
            self.fn(upload)

        out = pd.read_csv(tmp_path / f"raw_steam_{date.today()}.csv")
        assert len(out) == 3
        assert set(out["AppId"].astype(str)) == {"1", "2", "3"}

    def test_no_existing_file_creates_new(self, tmp_path):
        upload = self._make_steam_df([1, 2])

        nonexistent = tmp_path / "raw_steam_2020-01-01.csv"  # does not exist

        with patch("pipelines.steam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.steam_pipeline.get_latest_steam_csv", return_value=nonexistent):
            n_updated, n_new = self.fn(upload)

        assert n_new == 2
        assert n_updated == 0

    def test_date_appended_is_today(self, tmp_path):
        existing = self._make_steam_df([1])
        upload   = self._make_steam_df([2])

        existing_csv = tmp_path / "raw_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.steam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.steam_pipeline.get_latest_steam_csv", return_value=existing_csv):
            self.fn(upload)

        out = pd.read_csv(tmp_path / f"raw_steam_{date.today()}.csv")
        new_row = out[out["AppId"] == 2]
        assert new_row["date_appended"].iloc[0] == date.today().isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# 5. NON-STEAM PIPELINE MERGE  (pipelines/nonsteam_pipeline.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestAppendFromUploadedNonSteamCsv:
    """append_from_uploaded_nonsteam_csv — merge logic."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipelines.nonsteam_pipeline import append_from_uploaded_nonsteam_csv
        self.fn = append_from_uploaded_nonsteam_csv

    def _make_ns_df(self, titles):
        return pd.DataFrame({
            "Game Title": titles,
            "Category": ["Main Game"] * len(titles),
            "Release Date": ["2026-01-01"] * len(titles),
            "Developers": ["Dev A"] * len(titles),
            "Publishers": ["Pub A"] * len(titles),
            "Platforms": ["PC"] * len(titles),
            "Genres": ["Action"] * len(titles),
            "Themes": [""] * len(titles),
            "Keywords": [""] * len(titles),
            "YouTube URL": ["https://youtu.be/abc"] * len(titles),
            "YouTube Views": [1000] * len(titles),
            "YouTube Likes": [100] * len(titles),
            "YouTube ReleaseDate": ["2026-01-01"] * len(titles),
            "SteamStatus": ["Non-Steam PC Game"] * len(titles),
        })

    def test_all_new_rows_appended(self, tmp_path):
        existing = self._make_ns_df(["Alpha", "Beta"])
        upload   = self._make_ns_df(["Gamma", "Delta"])

        existing_csv = tmp_path / "raw_non_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.nonsteam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.nonsteam_pipeline.get_latest_nonsteam_csv", return_value=existing_csv):
            n_updated, n_new = self.fn(upload)

        assert n_new == 2
        assert n_updated == 0
        out = pd.read_csv(tmp_path / f"raw_non_steam_{date.today()}.csv")
        assert len(out) == 4

    def test_existing_titles_overwritten(self, tmp_path):
        existing = self._make_ns_df(["Alpha", "Beta"])
        upload   = self._make_ns_df(["Alpha"])
        upload["YouTube Views"] = [99999]  # changed data

        existing_csv = tmp_path / "raw_non_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.nonsteam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.nonsteam_pipeline.get_latest_nonsteam_csv", return_value=existing_csv):
            n_updated, n_new = self.fn(upload)

        assert n_updated == 1
        assert n_new == 0
        out = pd.read_csv(tmp_path / f"raw_non_steam_{date.today()}.csv")
        assert len(out) == 2
        alpha_row = out[out["Game Title"] == "Alpha"]
        assert alpha_row["YouTube Views"].iloc[0] == 99999

    def test_case_insensitive_dedup(self, tmp_path):
        existing = self._make_ns_df(["Alpha"])
        upload   = self._make_ns_df(["ALPHA"])  # same title, different case

        existing_csv = tmp_path / "raw_non_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.nonsteam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.nonsteam_pipeline.get_latest_nonsteam_csv", return_value=existing_csv):
            n_updated, n_new = self.fn(upload)

        assert n_updated == 1
        assert n_new == 0

    def test_untouched_rows_preserved(self, tmp_path):
        existing = self._make_ns_df(["Alpha", "Beta", "Gamma"])
        upload   = self._make_ns_df(["Alpha"])

        existing_csv = tmp_path / "raw_non_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.nonsteam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.nonsteam_pipeline.get_latest_nonsteam_csv", return_value=existing_csv):
            self.fn(upload)

        out = pd.read_csv(tmp_path / f"raw_non_steam_{date.today()}.csv")
        assert len(out) == 3

    def test_date_appended_is_today(self, tmp_path):
        existing = self._make_ns_df(["Alpha"])
        upload   = self._make_ns_df(["Beta"])

        existing_csv = tmp_path / "raw_non_steam_2026-01-01.csv"
        existing.to_csv(existing_csv, index=False)

        with patch("pipelines.nonsteam_pipeline.RAW_DIR", tmp_path), \
             patch("pipelines.nonsteam_pipeline.get_latest_nonsteam_csv", return_value=existing_csv):
            self.fn(upload)

        out = pd.read_csv(tmp_path / f"raw_non_steam_{date.today()}.csv")
        beta_row = out[out["Game Title"] == "Beta"]
        assert beta_row["date_appended"].iloc[0] == date.today().isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# 6. CONFIG  (config.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestGetLatestCsvHelpers:
    """get_latest_steam_csv / get_latest_nonsteam_csv — return most recent file."""

    def test_get_latest_steam_csv_picks_newest(self, tmp_path):
        (tmp_path / "raw_steam_2026-01-01.csv").touch()
        (tmp_path / "raw_steam_2026-06-01.csv").touch()

        import config
        with patch.object(config, "RAW_DIR", tmp_path):
            result = config.get_latest_steam_csv()

        assert result.name == "raw_steam_2026-06-01.csv"

    def test_get_latest_nonsteam_csv_picks_newest(self, tmp_path):
        (tmp_path / "raw_non_steam_2026-01-01.csv").touch()
        (tmp_path / "raw_non_steam_2026-05-10.csv").touch()

        import config
        with patch.object(config, "RAW_DIR", tmp_path):
            result = config.get_latest_nonsteam_csv()

        assert result.name == "raw_non_steam_2026-05-10.csv"

    def test_get_latest_steam_csv_falls_back_when_empty(self, tmp_path):
        import config
        with patch.object(config, "RAW_DIR", tmp_path):
            result = config.get_latest_steam_csv()
        assert result == config.CSV_STEAM

    def test_get_latest_nonsteam_csv_falls_back_when_empty(self, tmp_path):
        import config
        with patch.object(config, "RAW_DIR", tmp_path):
            result = config.get_latest_nonsteam_csv()
        assert result == config.CSV_NON_STEAM
