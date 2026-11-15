"""
Free Proxy Scraper & Tester for Event Cinemas + Village Cinemas
================================================================
Fetches free proxy lists, tests each proxy against both cinema endpoints,
and saves the working ones to a JSON file.

Run: python utils/fetchProxies.py
Output: utils/working_proxies.json
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime

import aiohttp
import requests
from fake_useragent import UserAgent

# ── CONFIG ───────────────────────────────────────────────────────────────────
PROXY_TEST_TIMEOUT   = 5       # seconds per proxy test (lower = faster sweep)
MAX_CONCURRENT_TESTS = 300     # async workers (hundreds are fine with aiohttp)
MAX_ACCEPTABLE_MS    = 3000    # discard proxies slower than this (ms)
OUTPUT_PATH          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "working_proxies.json")

# ── NEW TEST TARGETS ─────────────────────────────────────────────────────────
# Event Cinemas: lightweight JSON endpoint used by the site's front‑end.
EVENT_TEST_URL = "https://www.eventcinemas.com.au/Movies/GetNowShowing"

# Village Cinemas: root domain (HEAD request first, GET as fallback).
VILLAGE_TEST_URL = "https://villagecinemas.com.au/"

ua = UserAgent()

# ── PROXY SOURCES (unchanged) ────────────────────────────────────────────────
# ... (keep your existing source_github_lists, source_proxyscrape, source_geonode,
#      collect_all_proxies, _fetch_url functions exactly as they are)
# ---------------------------------------------------------------------------

# ── ASYNC PROXY TESTING ──────────────────────────────────────────────────────

async def _test_proxy_async(session, proxy_str, proxy_type, url, method="GET"):
    """
    Test a single proxy against a URL using aiohttp.
    Returns response time in ms or None on failure.
    """
    proxy_url = f"{proxy_type}://{proxy_str}" if "://" not in proxy_str else proxy_str
    headers = {"User-Agent": ua.random}
    try:
        start = time.monotonic()
        async with session.request(
            method,
            url,
            headers=headers,
            proxy=proxy_url,
            timeout=aiohttp.ClientTimeout(total=PROXY_TEST_TIMEOUT),
            allow_redirects=False,
        ) as resp:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            if resp.status == 200:
                # Optional: for Event JSON endpoint, ensure it's not a rate‑limit page
                if "eventcinemas" in url and "429" in await resp.text():
                    return None
                return elapsed_ms
    except Exception:
        pass
    return None


async def _test_single_proxy_async(session, proxy_str):
    """
    Test one proxy against Event and Village independently.
    Returns a result dict if it works for at least one platform.
    """
    for ptype in ("http", "socks5"):
        # Try Event (GET)
        event_ms = await _test_proxy_async(
            session, proxy_str, ptype, EVENT_TEST_URL, "GET"
        )
        event_ok = event_ms is not None and event_ms <= MAX_ACCEPTABLE_MS

        # Try Village (HEAD first, then GET)
        village_ms = await _test_proxy_async(
            session, proxy_str, ptype, VILLAGE_TEST_URL, "HEAD"
        )
        if village_ms is None:  # fallback to GET
            village_ms = await _test_proxy_async(
                session, proxy_str, ptype, VILLAGE_TEST_URL, "GET"
            )
        village_ok = village_ms is not None and village_ms <= MAX_ACCEPTABLE_MS

        if not event_ok and not village_ok:
            continue  # try next protocol

        if event_ok and village_ok:
            works_with = "both"
            avg = (event_ms + village_ms) // 2
        elif event_ok:
            works_with = "event_only"
            avg = event_ms
        else:
            works_with = "village_only"
            avg = village_ms

        return {
            "proxy": proxy_str,
            "type": ptype,
            "works_with": works_with,
            "event_ms": event_ms if event_ok else None,
            "village_ms": village_ms if village_ok else None,
            "avg_ms": avg,
            "tested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    return None


async def test_all_proxies_async(proxy_set):
    """
    Test all proxies concurrently using aiohttp.
    Returns a list of working proxy dicts sorted by speed.
    """
    total_before_skip = len(proxy_set)

    # Load already‑tested proxies to resume
    already_tested = set()
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            already_tested = set(data.get("tested", []))
        except Exception:
            pass

    to_test = proxy_set - already_tested
    skipped = total_before_skip - len(to_test)

    if skipped > 0:
        print(f"\n⏩ Resuming: skipping {skipped} already‑tested proxies")

    total = len(to_test)
    if total == 0:
        print("\n✅ All proxies already tested. Nothing to do.")
        data = _read_output_file()
        return data.get("proxies", [])

    print(f"\n🔍 Testing {total} proxies ({MAX_CONCURRENT_TESTS} concurrent, {PROXY_TEST_TIMEOUT}s timeout)...\n")

    working = []
    completed = 0
    start = time.monotonic()

    # Pre‑load working proxies from a partial previous run
    if os.path.exists(OUTPUT_PATH):
        data = _read_output_file()
        working = list(data.get("proxies", []))
        if working:
            print(f"   📂 Carrying forward {len(working)} working proxies from previous run")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_TESTS, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TESTS)

        async def bounded_test(proxy_str):
            async with semaphore:
                result = await _test_single_proxy_async(session, proxy_str)
                # Save result (thread‑safe wrapper kept from original)
                _save_proxy_result(proxy_str, result)
                return result

        tasks = [bounded_test(p) for p in to_test]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            if completed % 200 == 0 or completed == total:
                elapsed = time.monotonic() - start
                w = len(working)
                print(f"   📊 Progress: {completed}/{total} tested | {w} working | {elapsed:.0f}s elapsed")
            if result:
                working.append(result)
                proxy = result["proxy"]
                ptype = result["type"]
                tag = result["works_with"].upper()
                e_str = f"{result['event_ms']}ms" if result['event_ms'] is not None else "—"
                v_str = f"{result['village_ms']}ms" if result['village_ms'] is not None else "—"
                print(f"   ✅ {proxy} ({ptype}) [{tag}] — Event: {e_str}, Village: {v_str}")

    # Sort by average speed (fastest first)
    working.sort(key=lambda x: x["avg_ms"])
    return working


# ── HELPER: thread‑safe save (kept from original) ────────────────────────────
_file_lock = __import__("threading").Lock()  # keep your original logic

def _read_output_file():
    # ... (your existing implementation)
    ...

def _write_output_file(data):
    # ... (your existing implementation)
    ...

def _save_proxy_result(proxy_str, result=None):
    # ... (your existing implementation, using _file_lock)
    ...


# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Free Proxy Scraper & Tester (Event + Village Cinemas)")
    print("=" * 60)
    print()

    overall_start = time.monotonic()

    # 1. Collect from all sources
    all_proxies = collect_all_proxies()

    # 2. Add previously working proxies (re‑test them too)
    old_proxies = load_existing_proxies()
    all_proxies |= old_proxies

    if not all_proxies:
        print("❌ No proxies found from any source. Exiting.")
        sys.exit(1)

    # 3. Test all proxies (resumes from last offset if interrupted)
    working = asyncio.run(test_all_proxies_async(all_proxies))

    # 4. Final save — re‑sort by speed, keep tested list
    if working:
        data = _read_output_file()
        tested_list = data.get("tested", [])
        save_results(working, tested_list)
        both_count = sum(1 for p in working if p["works_with"] == "both")
        event_only = sum(1 for p in working if p["works_with"] == "event_only")
        village_only = sum(1 for p in working if p["works_with"] == "village_only")
        http_count = sum(1 for p in working if p["type"] == "http")
        socks_count = sum(1 for p in working if p["type"] == "socks5")

        print(f"\n📋 Summary:")
        print(f"   Total tested:      {len(all_proxies)}")
        print(f"   Working:           {len(working)}")
        print(f"     ├─ Both:         {both_count}")
        print(f"     ├─ Event only:   {event_only}")
        print(f"     └─ Village only:{village_only}")
        print(f"   HTTP proxies:      {http_count}")
        print(f"   SOCKS5 proxies:    {socks_count}")
        print(f"   Fastest:           {working[0]['proxy']} ({working[0]['avg_ms']}ms)")
        print(f"   Slowest kept:      {working[-1]['proxy']} ({working[-1]['avg_ms']}ms)")
    else:
        print("\n❌ No working proxies found. Try again later — free proxies are volatile.")

    elapsed = time.monotonic() - overall_start
    print(f"\n⏱ Total time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
