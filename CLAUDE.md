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
| `game_ranking/calculation/dataforseo_trends.py` | DataForSEO Google Trends client (category 41, worldwide, past 30 days, max 5 keywords/request, HTTP Basic auth) |
| `game_ranking/pipelines/steam_pipeline.py` | Gawk-3000 driver; `append_from_uploaded_steam_csv()` |
| `game_ranking/pipelines/nonsteam_pipeline.py` | SteamCommunityGroupScraper driver; `append_from_uploaded_nonsteam_csv()` |
| `game_ranking/pipelines/state.py` | Scrape window tracking |
| `game_ranking/pipelines/normalizer.py` | Encoding-safe CSV reading + per-upload normalization for Steam/Non-Steam |
| `game_ranking/pipelines/trends_pipeline.py` | Old blocking tournament pipeline; `load_tournament_anchor()` / `save_tournament_anchor()` still used by all tabs |
| `game_ranking/pipelines/tournament_state.py` | **NEW** — tournament state load/save/mutate; owns `cache/tournament_state.json` |
| `game_ranking/pipelines/tournament_pipeline.py` | **NEW** — `start_tournament()`, `submit_round()`, `collect_results()`; batch-POST per round |
| `game_ranking/calculation/trends_tournament.py` | Google Trends tournament engine (anchor-based comparison, groups of 8) |
| `game_ranking/app/tab_tournament.py` | Trends Tournament tab UI — auto-tournament (new batch/poll flow) + manual brackets |
| `SteamCommunityGroupScraper/script.py` | Non-steam scraper (Selenium + IGDB + YouTube) |
| `SteamCommunityGroupScraper/games.json` | Input game list (~846 entries) |

---

## Directory Layout

```
game_ranking/raw/     — Raw CSVs (see schema below)
game_ranking/data/    — Excel files + inventory CSV
game_ranking/cache/   — nonsteam_trends_cache.csv, steam_appid_cache.json,
                        player_counts_history.csv, steamspy_cache.csv, scraper_state.json,
                        dataforseo_creds.json, tournament_anchor.json, tournament_state.json,

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

**Latest raw file (as of 2026-05-10):**
`raw_steam_2026-05-10.csv` — 125 rows

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

**Latest raw file (as of 2026-05-10):**
`raw_non_steam_2026-05-10.csv` — 586 rows

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
- **Trends backend**: DataForSEO (`dataforseo_trends.py`); HTTP Basic auth
- **Tournament flow** (new): batch-POST per round; state persisted in `cache/tournament_state.json`; results collected via "Collect Results" button (polls `tasks_ready` + `task_get`); pingback_url set on every task; each bracket runs standard knockout until pool ≤ 5, final round top-2 = finalists; anchor pool = Steam top-2 + Non-Steam top-2 + all bye-games; user selects anchor from pool
- **Tournament state file**: `cache/tournament_state.json` — survives restarts, tracks rounds/tasks/scores/finalists per bracket
- **Anchor persistence**: `cache/tournament_anchor.json` — set from anchor pool selection, used by manual Fetch Trends buttons in other tabs
- **Trends results cache**: `cache/trends_results_cache.json` — keyword-level cache (30-day TTL); re-running the tournament with the same games skips DataForSEO API calls for groups already compared. Managed by `pipelines/trends_cache.py`. Task status `"cached"` means result came from cache, not API.
- **Old blocking pipeline** (`trends_pipeline.py`) still present; `load_tournament_anchor()` / `save_tournament_anchor()` reused by new pipeline and all tabs
- DataForSEO credentials stored in `cache/dataforseo_creds.json` (login + password, HTTP Basic auth)
- **Poll interval**: UI checks DataForSEO every 30s (was 120s)

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
