"""
Free Proxy Scraper & Tester for Event Cinemas + Village Cinemas
================================================================
Fetches free proxy lists, tests each proxy against both cinema endpoints,
and saves the working ones to a JSON file.

Run: python -u utils/fetchProxies.py
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
PROXY_TEST_TIMEOUT   = 5
MAX_CONCURRENT_TESTS = 300
MAX_ACCEPTABLE_MS    = 3000
OUTPUT_PATH          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "working_proxies.json")

EVENT_TEST_URL   = "https://www.eventcinemas.com.au/Movies/GetNowShowing"
VILLAGE_TEST_URL = "https://villagecinemas.com.au/"

ua = UserAgent()


# ── LOG HELPERS (all flush=True for CI live logs) ────────────────────────────

def _ts():
    return datetime.now().strftime("%H:%M:%S")


def log_info(msg):
    print(f"[{_ts()}] INFO   {msg}", flush=True)


def log_success(msg):
    print(f"[{_ts()}] OK     {msg}", flush=True)


def log_warn(msg):
    print(f"[{_ts()}] WARN   {msg}", flush=True)


def log_fail(msg):
    print(f"[{_ts()}] FAIL   {msg}", flush=True)


def log_debug(msg):
    if LOG_LEVEL == "DEBUG":
        print(f"[{_ts()}] DEBUG  {msg}", flush=True)


LOG_LEVEL = os.getenv("PROXY_LOG_LEVEL", "INFO").upper()


# ── PROXY SOURCES ────────────────────────────────────────────────────────────

def _fetch_url(url, label):
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": ua.random})
        r.raise_for_status()
        return r.text
    except Exception as e:
        log_warn(f"[{label}] source fetch failed: {type(e).__name__}: {e}")
        return ""


def source_github_lists():
    urls = [
    ]
    proxies = set()
    for label, url in urls:
        text = _fetch_url(url, label)
        count = 0
        for line in text.strip().splitlines():
            line = line.strip()
            if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+$', line):
                proxies.add(line)
                count += 1
        log_debug(f"[{label}] parsed {count} proxies")
    return proxies


def source_proxyscrape():
    proxies = set()
    for proto in ["http", "socks5"]:
        url = (
            "https://api.proxyscrape.com/v2/?request=displayproxies"
            f"&protocol={proto}&timeout=10000&country=all&ssl=all&anonymity=all"
        )
        text = _fetch_url(url, f"proxyscrape/{proto}")
        count = 0
        for line in text.strip().splitlines():
            line = line.strip()
            if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+$', line):
                proxies.add(line)
                count += 1
        log_debug(f"[proxyscrape/{proto}] parsed {count} proxies")
    return proxies


def source_geonode():
    proxies = set()
    try:
        url = "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc"
        text = _fetch_url(url, "geonode")
        if text:
            data = json.loads(text)
            for p in data.get("data", []):
                ip = p.get("ip")
                port = p.get("port")
                if ip and port:
                    proxies.add(f"{ip}:{port}")
            log_debug(f"[geonode] parsed {len(proxies)} proxies")
    except Exception as e:
        log_warn(f"[geonode] parse error: {type(e).__name__}: {e}")
    return proxies


def collect_all_proxies():
    log_info("Fetching proxies from all sources...")
    all_proxies = set()
    sources = [
        ("GitHub lists",  source_github_lists),
        ("ProxyScrape",   source_proxyscrape),
        ("GeoNode",       source_geonode),
    ]
    for name, fn in sources:
        try:
            found = fn()
            log_success(f"{name}: {len(found)} proxies")
            all_proxies |= found
        except Exception as e:
            log_fail(f"{name}: {type(e).__name__}: {e}")
    log_info(f"Total unique proxies collected: {len(all_proxies)}")
    return all_proxies


# ── FILE I/O ─────────────────────────────────────────────────────────────────

def _read_output_file():
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"generated_at": "", "total_working": 0, "total_tested": 0,
            "tested": [], "proxies": []}


def _write_output_file(data):
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ── BATCHED SAVE ─────────────────────────────────────────────────────────────
_pending_tested  = set()
_pending_working = []
_save_counter    = 0
FLUSH_EVERY      = 25


def _queue_save(proxy_str, result=None):
    global _save_counter
    _pending_tested.add(proxy_str)
    if result:
        _pending_working.append(result)
    _save_counter += 1
    if _save_counter >= FLUSH_EVERY:
        _flush_saves()


def _flush_saves():
    global _save_counter, _pending_tested, _pending_working
    if not _pending_tested and not _pending_working:
        return
    data = _read_output_file()
    tested_set = set(data.get("tested", [])) | _pending_tested
    data["tested"] = list(tested_set)
    data["total_tested"] = len(tested_set)
    if _pending_working:
        existing = {p["proxy"] for p in data["proxies"]}
        for r in _pending_working:
            if r["proxy"] not in existing:
                data["proxies"].append(r)
                existing.add(r["proxy"])
        data["total_working"] = len(data["proxies"])
    data["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_output_file(data)
    _pending_tested  = set()
    _pending_working = []
    _save_counter    = 0


# ── ASYNC PROXY TESTING ──────────────────────────────────────────────────────

async def _test_proxy_async(session, proxy_str, proxy_type, url, method="GET"):
    """
    Test a single proxy against one URL.
    Returns (elapsed_ms, reason) — elapsed_ms is None on failure.
    """
    proxy_url = f"{proxy_type}://{proxy_str}" if "://" not in proxy_str else proxy_str
    headers = {"User-Agent": ua.random}
    endpoint = "event" if "eventcinemas" in url else "village"
    try:
        start = time.monotonic()
        async with session.request(
            method, url,
            headers=headers,
            proxy=proxy_url,
            timeout=aiohttp.ClientTimeout(total=PROXY_TEST_TIMEOUT),
            allow_redirects=False,
        ) as resp:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            if resp.status == 200:
                if "eventcinemas" in url:
                    body = await resp.text()
                    if "429" in body or "rate limit" in body.lower():
                        return None, f"{endpoint} 200 but rate-limited body"
                return elapsed_ms, f"{endpoint} 200 OK"
            return None, f"{endpoint} HTTP {resp.status} ({elapsed_ms}ms)"
    except asyncio.TimeoutError:
        return None, f"{endpoint} timeout (>{PROXY_TEST_TIMEOUT}s)"
    except aiohttp.ClientProxyConnectionError as e:
        return None, f"{endpoint} proxy conn refused: {type(e).__name__}"
    except aiohttp.ClientConnectorError as e:
        return None, f"{endpoint} connector error: {type(e).__name__}"
    except aiohttp.ServerDisconnectedError:
        return None, f"{endpoint} server disconnected"
    except aiohttp.ClientError as e:
        return None, f"{endpoint} client error: {type(e).__name__}: {str(e)[:80]}"
    except Exception as e:
        return None, f"{endpoint} {type(e).__name__}: {str(e)[:80]}"


async def _test_single_proxy_async(session, proxy_str):
    """
    Test one proxy against both platforms across http/socks5.
    Logs each attempt and returns a result dict on success, None on failure.
    """
    attempt_log = []

    for ptype in ("http", "socks5"):
        # --- Event ---
        event_ms, event_reason = await _test_proxy_async(
            session, proxy_str, ptype, EVENT_TEST_URL, "GET"
        )
        event_ok = event_ms is not None and event_ms <= MAX_ACCEPTABLE_MS
        if event_ms is not None and not event_ok:
            event_reason = f"event too slow ({event_ms}ms > {MAX_ACCEPTABLE_MS}ms)"

        # --- Village (HEAD, fall back to GET) ---
        village_ms, village_reason = await _test_proxy_async(
            session, proxy_str, ptype, VILLAGE_TEST_URL, "HEAD"
        )
        if village_ms is None:
            village_ms, village_reason = await _test_proxy_async(
                session, proxy_str, ptype, VILLAGE_TEST_URL, "GET"
            )
        village_ok = village_ms is not None and village_ms <= MAX_ACCEPTABLE_MS
        if village_ms is not None and not village_ok:
            village_reason = f"village too slow ({village_ms}ms > {MAX_ACCEPTABLE_MS}ms)"

        attempt_log.append(
            f"{ptype} → event={event_reason} | village={village_reason}"
        )

        if not event_ok and not village_ok:
            log_debug(f"{proxy_str} [{ptype}] rejected: {attempt_log[-1]}")
            continue

        # At least one works — build result
        if event_ok and village_ok:
            works_with = "both"
            avg = (event_ms + village_ms) // 2
        elif event_ok:
            works_with = "event_only"
            avg = event_ms
        else:
            works_with = "village_only"
            avg = village_ms

        result = {
            "proxy":      proxy_str,
            "type":       ptype,
            "works_with": works_with,
            "event_ms":   event_ms   if event_ok   else None,
            "village_ms": village_ms if village_ok else None,
            "avg_ms":     avg,
            "tested_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        e_str = f"{event_ms}ms"   if event_ok   else "—"
        v_str = f"{village_ms}ms" if village_ok else "—"
        if works_with == "both":
            log_success(f"{proxy_str} [{ptype}] BOTH         event={e_str} village={v_str} avg={avg}ms")
        else:
            log_warn(f"{proxy_str} [{ptype}] {works_with.upper():<12} event={e_str} village={v_str}")

        return result

    # All protocols failed
    log_fail(f"{proxy_str} — all protocols failed | " + " || ".join(attempt_log))
    return None


async def test_all_proxies_async(proxy_set):
    total_before_skip = len(proxy_set)

    already_tested = set()
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                already_tested = set(json.load(f).get("tested", []))
        except Exception:
            pass

    to_test = proxy_set - already_tested
    skipped = total_before_skip - len(to_test)
    if skipped > 0:
        log_info(f"Resuming: skipping {skipped} already-tested proxies")

    total = len(to_test)
    if total == 0:
        log_success("All proxies already tested. Nothing to do.")
        return _read_output_file().get("proxies", [])

    log_info(
        f"Testing {total} proxies | concurrency={MAX_CONCURRENT_TESTS} | "
        f"timeout={PROXY_TEST_TIMEOUT}s | max_acceptable={MAX_ACCEPTABLE_MS}ms"
    )

    working   = []
    completed = 0
    start     = time.monotonic()

    if os.path.exists(OUTPUT_PATH):
        working = list(_read_output_file().get("proxies", []))
        if working:
            log_info(f"Carrying forward {len(working)} working proxies from previous run")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_TESTS, ttl_dns_cache=300)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        async def bounded_test(proxy_str):
            async with semaphore:
                result = await _test_single_proxy_async(session, proxy_str)
                _queue_save(proxy_str, result)
                return result

        tasks = [asyncio.create_task(bounded_test(p)) for p in to_test]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1
            if completed % 100 == 0 or completed == total:
                elapsed = time.monotonic() - start
                rate = completed / elapsed if elapsed > 0 else 0
                log_info(
                    f"Progress {completed}/{total} | "
                    f"working={len(working)} | "
                    f"{rate:.1f}/s | "
                    f"{elapsed:.0f}s elapsed"
                )
            if result:
                working.append(result)

    _flush_saves()
    working.sort(key=lambda x: x["avg_ms"])
    return working


# ── FINAL SAVE ───────────────────────────────────────────────────────────────

def save_results(working, tested_list=None):
    output = {
        "generated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_working": len(working),
        "total_tested":  len(tested_list) if tested_list else 0,
        "tested":        tested_list or [],
        "proxies":       working,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log_success(f"Saved {len(working)} working proxies (sorted by speed) to: {OUTPUT_PATH}")


def load_existing_proxies():
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                old = json.load(f).get("proxies", [])
            log_info(f"Loaded {len(old)} previously working proxies for re-testing")
            return {p["proxy"] for p in old}
        except Exception:
            pass
    return set()


# ── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70, flush=True)
    print("  Free Proxy Scraper & Tester (Event + Village Cinemas)", flush=True)
    print("=" * 70, flush=True)
    print(flush=True)

    overall_start = time.monotonic()

    all_proxies = collect_all_proxies()
    all_proxies |= load_existing_proxies()

    if not all_proxies:
        log_fail("No proxies found from any source. Exiting.")
        sys.exit(1)

    working = asyncio.run(test_all_proxies_async(all_proxies))

    if working:
        data = _read_output_file()
        save_results(working, data.get("tested", []))

        both_count   = sum(1 for p in working if p["works_with"] == "both")
        event_only   = sum(1 for p in working if p["works_with"] == "event_only")
        village_only = sum(1 for p in working if p["works_with"] == "village_only")
        http_count   = sum(1 for p in working if p["type"] == "http")
        socks_count  = sum(1 for p in working if p["type"] == "socks5")

        print(flush=True)
        log_info("─── SUMMARY ────────────────────────────────────────────")
        log_info(f"Total tested:       {len(all_proxies)}")
        log_info(f"Working:            {len(working)}")
        log_info(f"  ├─ Both:          {both_count}")
        log_info(f"  ├─ Event only:    {event_only}")
        log_info(f"  └─ Village only:  {village_only}")
        log_info(f"HTTP proxies:       {http_count}")
        log_info(f"SOCKS5 proxies:     {socks_count}")
        log_info(f"Fastest:            {working[0]['proxy']} ({working[0]['avg_ms']}ms)")
        log_info(f"Slowest kept:       {working[-1]['proxy']} ({working[-1]['avg_ms']}ms)")
    else:
        log_fail("No working proxies found. Try again later — free proxies are volatile.")

    elapsed = time.monotonic() - overall_start
    log_info(f"Total time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
