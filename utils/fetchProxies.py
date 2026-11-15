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
PROXY_TEST_TIMEOUT   = 5       # seconds per proxy test
MAX_CONCURRENT_TESTS = 300     # async workers
MAX_ACCEPTABLE_MS    = 3000    # discard proxies slower than this
OUTPUT_PATH          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "working_proxies.json")

EVENT_TEST_URL   = "https://www.eventcinemas.com.au/Movies/GetNowShowing"
VILLAGE_TEST_URL = "https://villagecinemas.com.au/"

ua = UserAgent()


# ── PROXY SOURCES ────────────────────────────────────────────────────────────

def _fetch_url(url, label):
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": ua.random})
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"   ⚠ [{label}] Failed: {e}")
        return ""


def source_github_lists():
    urls = [
        ("TheSpeedX/http",   "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"),
        ("TheSpeedX/socks5", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt"),
        ("clarketm",         "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt"),
        ("monosans/http",    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"),
        ("monosans/socks5",  "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks5.txt"),
        ("hookzof",          "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),
        ("roosterkid",       "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt"),
        ("MuRongPIG/http",   "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt"),
        ("MuRongPIG/socks5", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt"),
    ]
    proxies = set()
    for label, url in urls:
        text = _fetch_url(url, label)
        for line in text.strip().splitlines():
            line = line.strip()
            if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+$', line):
                proxies.add(line)
    return proxies


def source_proxyscrape():
    proxies = set()
    for proto in ["http", "socks5"]:
        url = (
            "https://api.proxyscrape.com/v2/?request=displayproxies"
            f"&protocol={proto}&timeout=10000&country=all&ssl=all&anonymity=all"
        )
        text = _fetch_url(url, f"proxyscrape/{proto}")
        for line in text.strip().splitlines():
            line = line.strip()
            if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+$', line):
                proxies.add(line)
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
    except Exception as e:
        print(f"   ⚠ [geonode] Parse error: {e}")
    return proxies


def collect_all_proxies():
    print("📡 Fetching proxies from all sources...\n")
    all_proxies = set()
    sources = [
        ("GitHub lists",  source_github_lists),
        ("ProxyScrape",   source_proxyscrape),
        ("GeoNode",       source_geonode),
    ]
    for name, fn in sources:
        try:
            found = fn()
            print(f"   ✅ {name}: {len(found)} proxies")
            all_proxies |= found
        except Exception as e:
            print(f"   ❌ {name}: {e}")
    print(f"\n📊 Total unique proxies collected: {len(all_proxies)}")
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


# ── BATCHED SAVE (non-blocking resume support) ───────────────────────────────
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
    proxy_url = f"{proxy_type}://{proxy_str}" if "://" not in proxy_str else proxy_str
    headers = {"User-Agent": ua.random}
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
                        return None
                return elapsed_ms
    except Exception:
        pass
    return None


async def _test_single_proxy_async(session, proxy_str):
    for ptype in ("http", "socks5"):
        event_ms = await _test_proxy_async(session, proxy_str, ptype, EVENT_TEST_URL, "GET")
        event_ok = event_ms is not None and event_ms <= MAX_ACCEPTABLE_MS

        village_ms = await _test_proxy_async(session, proxy_str, ptype, VILLAGE_TEST_URL, "HEAD")
        if village_ms is None:
            village_ms = await _test_proxy_async(session, proxy_str, ptype, VILLAGE_TEST_URL, "GET")
        village_ok = village_ms is not None and village_ms <= MAX_ACCEPTABLE_MS

        if not event_ok and not village_ok:
            continue

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
            "proxy":      proxy_str,
            "type":       ptype,
            "works_with": works_with,
            "event_ms":   event_ms   if event_ok   else None,
            "village_ms": village_ms if village_ok else None,
            "avg_ms":     avg,
            "tested_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
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
        print(f"\n⏩ Resuming: skipping {skipped} already-tested proxies")

    total = len(to_test)
    if total == 0:
        print("\n✅ All proxies already tested. Nothing to do.")
        return _read_output_file().get("proxies", [])

    print(f"\n🔍 Testing {total} proxies ({MAX_CONCURRENT_TESTS} concurrent, "
          f"{PROXY_TEST_TIMEOUT}s timeout)...\n")

    working   = []
    completed = 0
    start     = time.monotonic()

    if os.path.exists(OUTPUT_PATH):
        working = list(_read_output_file().get("proxies", []))
        if working:
            print(f"   📂 Carrying forward {len(working)} working proxies from previous run")

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
            if completed % 200 == 0 or completed == total:
                elapsed = time.monotonic() - start
                print(f"   📊 Progress: {completed}/{total} tested | "
                      f"{len(working)} working | {elapsed:.0f}s elapsed")
            if result:
                working.append(result)
                tag   = result["works_with"].upper()
                e_str = f"{result['event_ms']}ms"   if result['event_ms']   is not None else "—"
                v_str = f"{result['village_ms']}ms" if result['village_ms'] is not None else "—"
                print(f"   ✅ {result['proxy']} ({result['type']}) [{tag}] "
                      f"— Event: {e_str}, Village: {v_str}")

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
    print(f"\n💾 Saved {len(working)} working proxies (sorted by speed) to: {OUTPUT_PATH}")


def load_existing_proxies():
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                old = json.load(f).get("proxies", [])
            print(f"📂 Loaded {len(old)} previously working proxies for re-testing")
            return {p["proxy"] for p in old}
        except Exception:
            pass
    return set()


# ── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Free Proxy Scraper & Tester (Event + Village Cinemas)")
    print("=" * 60)
    print()

    overall_start = time.monotonic()

    all_proxies = collect_all_proxies()
    all_proxies |= load_existing_proxies()

    if not all_proxies:
        print("❌ No proxies found from any source. Exiting.")
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

        print(f"\n📋 Summary:")
        print(f"   Total tested:      {len(all_proxies)}")
        print(f"   Working:           {len(working)}")
        print(f"     ├─ Both:         {both_count}")
        print(f"     ├─ Event only:   {event_only}")
        print(f"     └─ Village only: {village_only}")
        print(f"   HTTP proxies:      {http_count}")
        print(f"   SOCKS5 proxies:    {socks_count}")
        print(f"   Fastest:           {working[0]['proxy']} ({working[0]['avg_ms']}ms)")
        print(f"   Slowest kept:      {working[-1]['proxy']} ({working[-1]['avg_ms']}ms)")
    else:
        print("\n❌ No working proxies found. Try again later — free proxies are volatile.")

    elapsed = time.monotonic() - overall_start
    print(f"\n⏱ Total time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
