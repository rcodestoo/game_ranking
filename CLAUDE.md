# AGS Game Ranking Tool — CLAUDE.md

Internal Streamlit tool for an AGS game review team to prioritize which games to review.
Covers Steam and Non-Steam games. Root: `C:\Users\Rasika\Desktop\AGS\repos\`

---

## Repo Structure

```
repos/
  game_ranking/           ← Main Python project (Streamlit app)
  Release-Gawk-3000/      ← C# Steam scraper (Gawk-3000)
  SteamCommunityGroupScraper/  ← Python non-steam scraper (script.py)
  venv/                   ← Python virtualenv
```

---

## Key Files

| File | Role |
|------|------|
| `game_ranking/streamlit_app.py` | 9-line wrapper, delegates to `app/main.py` |
| `game_ranking/app/main.py` | Page config, session init, sidebar uploads, tab rendering |
| `game_ranking/app/tab_steam.py` | Steam tab UI |
| `game_ranking/app/tab_nonsteam.py` | Non-Steam tab UI |
| `game_ranking/app/tab_inventory.py` | Inventory tab UI |
| `game_ranking/app/helpers.py` | `highlight_new_rows()`, `load_defaults()`, reload helpers |
| `game_ranking/app/thread_state.py` | Thread state dicts (NOT session_state) |
| `game_ranking/config.py` | Centralized path constants; `get_latest_steam_csv()`, `get_latest_nonsteam_csv()` |
| `game_ranking/calculation/process_data.py` | Scoring logic + `populate_appids()` |
| `game_ranking/calculation/steam_players.py` | SteamSpy + Steam API + AppID cache |
| `game_ranking/calculation/scraper.py` | pytrends wrapper |
| `game_ranking/pipelines/steam_pipeline.py` | Gawk-3000 driver; `append_from_uploaded_steam_csv()` |
| `game_ranking/pipelines/nonsteam_pipeline.py` | SteamCommunityGroupScraper driver; `append_from_uploaded_nonsteam_csv()` |
| `game_ranking/pipelines/state.py` | Scrape window tracking |
| `SteamCommunityGroupScraper/script.py` | Non-steam scraper (Selenium + IGDB + YouTube) |
| `SteamCommunityGroupScraper/games.json` | Input game list (~846 entries) |

---

## Directory Layout

```
game_ranking/raw/     — Raw CSVs (see schema below)
game_ranking/data/    — Excel files + inventory CSV
game_ranking/cache/   — nonsteam_trends_cache.csv, steam_appid_cache.json,
                        player_counts_history.csv, steamspy_cache.csv, scraper_state.json
```

---

## Raw CSV Files

### Steam (`raw_steam.csv` / `raw_steam_YYYY-MM-DD.csv`)

Produced by Gawk-3000 and ingested via `append_from_uploaded_steam_csv()`.

**Canonical columns (as stored):**
```
AppId, Name, FollowerCount, Genres, Categories,
IGDB_Genres, IGDB_Themes, IGDB_Keywords,
ExactMatch, ScrapedName, ReleaseDate, ParsedDate,
YoutubeURL, YoutubeViewCount, YoutubeLikeCount,
ScrapingError, Developers, IsFullyProcessed, Publishers, ReleaseInfo,
date_appended
```

**Minimum required for upload (validated in sidebar):**
`Name, FollowerCount, Developers, Genres, ReleaseDate`

**Latest new raw file (as of 2026-04-23):**
`steam_export_full_combined_filtered_23_4.csv` — 151 rows, Gawk-3000 export

### Non-Steam (`raw_non_steam.csv` / `raw_non_steam_YYYY-MM-DD.csv`)

Produced by SteamCommunityGroupScraper and ingested via `append_from_uploaded_nonsteam_csv()`.
Normalized by `_normalize_nonsteam_df()` in `nonsteam_pipeline.py`.

**Canonical columns (enforced):**
```
Game Title, Category, Release Date, Developers, Publishers,
Platforms, Genres, Themes, Keywords,
YouTube URL, YouTube Views, YouTube Likes, YouTube ReleaseDate,
SteamStatus, date_appended
```

`date_appended` is added on upload (not present in raw scraper output).

**Minimum required for upload (validated in sidebar):**
`Game Title, Developers, SteamStatus, YouTube Views`

**Latest new raw file (as of 2026-04-23):**
`Categorized_Game_List_2026-04-15_to_2026-05-16.csv` — 553 rows, covers 2026-04-15 to 2026-05-16

---

## Pipeline Notes

- Scraper background threads are **disabled** in UI (commented out in tab files)
- `input()` is monkey-patched in `nonsteam_pipeline.py` to avoid interactive prompts
- Steam pipeline uses `dotnet run` on `Release-Gawk-3000/Gawk-3000/`
- Non-steam pipeline dynamically loads `SteamCommunityGroupScraper/script.py`
- Upload flow: sidebar file uploader → preview → "Load" button → pipeline function → `reload_*_from_csv()`
- Deduplication key: **AppId** for Steam, **Game Title** (case-insensitive) for Non-Steam
- On upload, existing rows with matching key are **overwritten**; new rows are **appended**
- Output always written to a date-stamped file (`raw_steam_YYYY-MM-DD.csv`)

---

## UI Preferences

- Date pickers use `format="DD/MM/YYYY"` across all tabs
- ReleaseDate displayed as `dd/mm/yyyy` via `.dt.strftime('%d/%m/%Y')` in display df
- Highlight: yellow background `#fff59d`, black text `#000000`
- Filters default to collapsed (`expanded=False`)
- New rows (`date_appended == today`) highlighted yellow via `helpers.highlight_new_rows()`
- Global date range computed from all three tabs and shared across them

---

## Session State Keys

| Key | Contents |
|-----|----------|
| `df_steam` | Processed Steam DataFrame |
| `df_nonsteam` | Processed Non-Steam DataFrame |
| `steam_cleaned` / `nonsteam_cleaned` | Whether scoring has been applied |
| `uploaded_steam_bytes` / `uploaded_steam_name` | Sidebar upload cache |
| `uploaded_nonsteam_bytes` / `uploaded_nonsteam_name` | Sidebar upload cache |
| `player_count_history` | From `fetch_player_counts_if_needed()` |
| `nonsteam_trends` | Dict of `{game_name: trends_score}` |
| `trends_last_fetched_at` | Timestamp string from trends cache |
| `game_data` | Inventory DataFrame |
| `dev_list` / `genre_list` | Filter lists from `load_data()` |
