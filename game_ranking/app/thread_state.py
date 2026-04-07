# Module-level shared state for background scraper threads.
# st.session_state is NOT accessible from background threads, so threads
# write their result here and the main thread reads it on the next rerun.

_ns_thread_state: dict = {"result": None, "running": False}
_steam_thread_state: dict = {"result": None, "running": False}
_ns_verify_thread_state: dict = {"running": False, "result": None, "total": 0, "done": 0}
