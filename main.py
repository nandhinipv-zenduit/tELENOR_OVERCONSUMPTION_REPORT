import os
import io
import csv
import json
import time
import gzip
import zipfile
import asyncio
import aiohttp
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from aiohttp import ClientTimeout
from yarl import URL
from tqdm.asyncio import tqdm_asyncio
import smtplib
from email.message import EmailMessage
from io import BytesIO

# ==========================================================
# WHAT THIS IS
# ==========================================================
# Telenor sibling to the 1NCE overconsumption script (daily_sim_report.py),
# restructured to match that script's shape: env-var credentials, a 31-day
# trailing usage window (T31 -> T1) compared against the Zenduit data plan,
# an in-memory Excel report, and the same Gmail delivery.
#
# THE PLATFORM: Telenor (serviceportal.telenorconnexion.com) runs on
# Ericsson IoT Accelerator, white-labeled by Aeris. Docs live at
# https://iotdeveloper.aeris.net.
#   1) Auth:          POST /iot/api/auth/token           (OAuth2 password grant)
#   2) Subscribers:   GET  /iot/api/subscriptions/details (cursor-paginated)
#   3) Usage:         Business Analytics Report API — a REPORT-FILE pattern:
#                      GET /usages (list files for a day) -> GET /usages/{id}
#                      (download the file, which is a ZIP containing a CSV).
#   4) AccountId / account name mapping: sourced from Zoho Analytics (two
#      views — "Zenduit Devices" and "Accounts"), same as the 1NCE script's
#      Zenduit-side data, since Telenor devices aren't provisioned in the
#      Zenduit admin API the 1NCE script talks to.
#
# ⚠️ STILL UNCONFIRMED (see comments at each site below):
#   - The usage file's internal column names for ICCID/IMSI/volume are
#     auto-detected from a candidate list rather than hard-coded, because
#     the OpenAPI docs only document the *listing* endpoint's metadata.
#   - Which of the two candidate company query values
#     (TELENOR_COMPANY_QUERY) is correct for this account.
#   - Whether the usage-file byte volume is bytes (assumed) or already KB/MB.
# ==========================================================

# ==========================================================
# GMAIL CONFIG (GLOBAL) — identical to the 1NCE script.
# ==========================================================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_SENDER = os.getenv("GMAIL_USERNAME")
EMAIL_PASSWORD = os.getenv("GMAIL_PASS")
EMAIL_TO = [
    "nandhinipv@zenduit.com","abidali@gofleet.com","nikithavinod@zenduit.com","rizamae@gofleet.com"
]
if not EMAIL_SENDER or not EMAIL_PASSWORD:
    raise RuntimeError("❌ Gmail credentials not found in environment variables")

# ==========================================================
# TELENOR / AERIS CONFIG
# ==========================================================
AERIS_BASE = "https://iot-api.aeris.com/iot/api"
AERIS = {
    "token_url": f"{AERIS_BASE}/auth/token",
    "subscriptions_url": f"{AERIS_BASE}/subscriptions/details",
    "analytics_base": f"{AERIS_BASE}/business-analytics-service/v1",
    # Fixed public value confirmed from the Auth API docs — not something
    # you generate or request from Telenor.
    "client_id": "cm-public-api-client",
    # Your Service Portal login (or a dedicated service-account login).
    "username": os.getenv("TELENOR_USERNAME"),
    "password": os.getenv("TELENOR_PASSWORD"),
}
if not AERIS["username"] or not AERIS["password"]:
    raise RuntimeError("❌ Telenor credentials not found in environment variables "
                        "(TELENOR_USERNAME / TELENOR_PASSWORD)")

# Company / customer number used to build the REQUIRED `q` filter on the
# Subscription Search API (q expressions use a double equals, e.g.
# "company==01001566"). Two candidates showed up during discovery —
# "01001566" (from the Service Portal URL) and "01001155" (referenced in
# custom role names). Override via env var if the default is wrong.
COMPANY_QUERY = os.getenv("TELENOR_COMPANY_QUERY", "01001566")

PAGE_LIMIT = 10000  # Subscription Search API's documented max for `limit`
USAGE_CONCURRENCY = 4  # concurrent day-fetches against the usage report API

# ==========================================================
# ZOHO ANALYTICS CONFIG — same source as the 1NCE/FloLive scripts use for
# Zenduit-side data (no calls to Zenduit's own website/API for Telenor).
# ==========================================================
ZOHO_ANALYTICS_DOMAIN = os.getenv("ZOHO_ANALYTICS_DOMAIN", "analyticsapi.zoho.com")
ZOHO_ACCOUNTS_DOMAIN = os.getenv("ZOHO_ACCOUNTS_DOMAIN", "accounts.zoho.com")
ZOHO_ANALYTICS_API = f"https://{ZOHO_ANALYTICS_DOMAIN}/restapi/v2"
ZOHO_ORG_ID = os.getenv("ZOHO_ORG_ID", "67409019")
ZOHO_ANALYTICS_WORKSPACE_ID = "953790000013364003"
ZOHO_ANALYTICS_ZENDUIT_DEVICES_VIEW_ID = "953790000054827175"   # "Zenduit Devices" view
ZOHO_ANALYTICS_ACCOUNTS_VIEW_ID = "953790000013364024"          # "Accounts" view
ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS = int(os.getenv("ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS", "300"))


def _pick_env(*names):
    """First-match-wins env var lookup that also reports which var supplied
    the value — matters because falling back to generic ZOHO_* (CRM) vars
    authenticates fine but gets rejected by Analytics with INVALID_OAUTHSCOPE."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v, n
    return None, None


ZOHO_ANALYTICS_CLIENT_ID, _ZOHO_CLIENT_ID_SRC = _pick_env(
    "ZOHO_CLIENT_ID_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_ID", "ZOHO_CLIENT_ID")
ZOHO_ANALYTICS_CLIENT_SECRET, _ZOHO_CLIENT_SECRET_SRC = _pick_env(
    "ZOHO_CLIENT_SECRET_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_SECRET", "ZOHO_CLIENT_SECRET")
ZOHO_ANALYTICS_REFRESH_TOKEN, _ZOHO_REFRESH_TOKEN_SRC = _pick_env(
    "ZOHO_CLIENT_REFRESH_TOKEN_ANALYTICS", "ZOHO_ANALYTICS_REFRESH_TOKEN", "ZOHO_REFRESH_TOKEN")

_ZOHO_ANALYTICS_SPECIFIC = {
    "ZOHO_CLIENT_ID_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_ID",
    "ZOHO_CLIENT_SECRET_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_SECRET",
    "ZOHO_CLIENT_REFRESH_TOKEN_ANALYTICS", "ZOHO_ANALYTICS_REFRESH_TOKEN",
}


def _report_zoho_credential_sources():
    srcs = {
        "client_id": _ZOHO_CLIENT_ID_SRC,
        "client_secret": _ZOHO_CLIENT_SECRET_SRC,
        "refresh_token": _ZOHO_REFRESH_TOKEN_SRC,
    }
    print(f"Zoho credentials sourced from env vars: {srcs}")

    unset = [k for k, v in srcs.items() if v is None]
    if unset:
        print(f"⚠️ No env var found at all for: {unset}. Copy the Zoho env vars from "
              f"daily_sim_report.py's run config if this is a different run config.")
        return

    generic = {k: v for k, v in srcs.items() if v not in _ZOHO_ANALYTICS_SPECIFIC}
    if generic:
        print(f"⚠️ These fell back to GENERIC Zoho env vars rather than Analytics-specific "
              f"ones: {generic}. If those hold Zoho CRM credentials, the export calls below "
              f"will fail with INVALID_OAUTHSCOPE (errorCode 8540).")


# ==========================================================
# SHARED HELPERS
# ==========================================================
def _numeric_fillna(series, fill=0):
    return pd.to_numeric(series, errors="coerce").fillna(fill)


def _find_col(df, candidates):
    lower_map = {str(c).lower(): c for c in df.columns if c is not None}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


# ==========================================================
# AERIS/IoT ACCELERATOR AUTH — OAuth2 "password" grant.
# ==========================================================
async def get_aeris_token():
    async with aiohttp.ClientSession() as session:
        async with session.post(
            AERIS["token_url"],
            data={
                "grant_type": "password",
                "client_id": AERIS["client_id"],
                "username": AERIS["username"],
                "password": AERIS["password"],
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/vnd.dcp-v1+json",
            },
            timeout=ClientTimeout(total=30),
        ) as r:
            if r.status != 200:
                text = await r.text()
                raise RuntimeError(f"Aeris/Telenor auth failed: {r.status} — {text[:500]}")
            data = await r.json()

    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Aeris/Telenor auth: no access_token in response: {data}")
    return token


# ==========================================================
# SUBSCRIBERS (BASE) — Subscription Search API.
# `q` is effectively REQUIRED and must use a double-equals comparison,
# e.g. "company==01001566" — a bare or colon-separated filter gets a
# misleading "query string must contain equals" 400.
# ==========================================================
async def fetch_all_subscribers(token):
    if not COMPANY_QUERY:
        raise RuntimeError(
            "COMPANY_QUERY is not set. The Subscription Search API rejects calls without a "
            "`q` filter. Set TELENOR_COMPANY_QUERY — try '01001566' first, then '01001155'."
        )

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    cursor = None
    rows = []
    use_additional_fields = True

    async with aiohttp.ClientSession() as session:
        while True:
            qs = [
                f"limit={PAGE_LIMIT}",
                f"q=company=={quote(str(COMPANY_QUERY), safe='')}",
            ]
            if use_additional_fields:
                qs += [
                    "additionalFields=MONTHLY_DATA",
                    "additionalFields=DATES",
                    "additionalFields=LABEL",
                    "additionalFields=PACKAGE",
                ]
            if cursor:
                qs.append(f"cursor={quote(str(cursor), safe='')}")
            request_url = URL(f"{AERIS['subscriptions_url']}?{'&'.join(qs)}", encoded=True)

            async with session.get(request_url, headers=headers, timeout=ClientTimeout(total=60)) as r:
                if r.status != 200:
                    text = await r.text()
                    if use_additional_fields:
                        print(f"⚠️ additionalFields rejected ({r.status}: {text[:200]}) — "
                              f"retrying without them.")
                        use_additional_fields = False
                        continue
                    raise RuntimeError(f"Subscription Search API failed: {r.status} — {text[:500]}")
                data = await r.json()

            page = data.get("subscriptions") or []
            rows.extend(page)
            cursor = data.get("next")
            if not cursor or not page:
                break

    print(f"✅ Total Telenor SIMs fetched: {len(rows)}")
    if not rows:
        return pd.DataFrame(columns=["ICCID", "IMSI", "MSISDN", "SIM_Status",
                                      "MonthlyData_Bytes", "Enterprise", "Label"])

    df = pd.DataFrame(rows)
    out = pd.DataFrame()
    out["ICCID"] = df.get("iccid", pd.Series(dtype=str)).astype(str).str.strip()
    out["IMSI"] = df.get("imsi", pd.Series(dtype=str)).astype(str).str.strip()
    out["MSISDN"] = df.get("msisdn")
    out["SIM_Status"] = df.get("state")
    out["MonthlyData_Bytes"] = df.get("monthlyData")
    out["Enterprise"] = df.get("enterprise")
    out["Label"] = df.get("label")
    return out.drop_duplicates(subset=["ICCID"], keep="first")


# ==========================================================
# USAGE — Business Analytics Report API, report-file pattern, summed over
# the same 31-day trailing window (T31 -> T1) the 1NCE script uses so the
# comparison against Zenduit_Data_Plan is apples-to-apples.
# ==========================================================
_USAGE_ICCID_CANDIDATES = ["iccid", "ICCID", "Iccid", "SIM", "simId"]
_USAGE_IMSI_CANDIDATES = ["imsi", "IMSI"]
_USAGE_QUANTITY_CANDIDATES = [
    "volume", "usage", "dataUsage", "data_usage", "quantity", "bytes", "totalBytes",
    "dataVolume", "value", "totalVolume", "kb", "KB", "mb", "MB",
]


def _rows_from_csv_text(text):
    sample = text[:8192]
    delimiter = ","
    try:
        delimiter = csv.Sniffer().sniff(sample, delimiters=",;|\t").delimiter
    except Exception:
        pass
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter, restkey="_extra_values")
    return [row for row in reader]


def _parse_usage_payload(raw, file_id):
    """Confirmed live: despite an octet-stream Content-Type, these files are
    ZIP archives containing a CSV named like '1001566GPRS20260804.csv'."""
    if not raw:
        return []

    if raw[:4] == b"PK\x03\x04":
        rows = []
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                for name in [n for n in zf.namelist() if not n.endswith("/")]:
                    data = zf.read(name)
                    if data[:2] == b"\x1f\x8b":
                        data = gzip.decompress(data)
                    text = data.decode("utf-8-sig", errors="replace")
                    rows.extend(_rows_from_csv_text(text))
        except zipfile.BadZipFile as e:
            print(f"⚠️ usage file {file_id} looked like a ZIP but couldn't be opened: {e}")
        return rows

    if raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except OSError as e:
            print(f"⚠️ usage file {file_id} looked gzipped but wouldn't decompress: {e}")
            return []

    text = raw.decode("utf-8-sig", errors="replace")
    stripped = text.lstrip()
    if stripped[:1] in ("{", "["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as e:
            print(f"⚠️ usage file {file_id} looked like JSON but wouldn't parse: {e}")
            return []
        recs = parsed.get("data") if isinstance(parsed, dict) and "data" in parsed else parsed
        if isinstance(recs, dict):
            recs = [recs]
        return recs or []

    return _rows_from_csv_text(text)


def _usage_window_candidates(day):
    """Confirmed by probing: the API wants midnight-aligned timestamps with
    no milliseconds. The confirmed winner is listed first; the rest are
    fallbacks kept in case the accepted shape ever changes."""
    nxt = day + timedelta(days=1)
    return [
        ("midnight -> next midnight (no ms)  [CONFIRMED WORKING]",
         day.strftime("%Y-%m-%dT00:00:00Z"), nxt.strftime("%Y-%m-%dT00:00:00Z")),
        ("midnight -> next midnight (ms)",
         day.strftime("%Y-%m-%dT00:00:00.000Z"), nxt.strftime("%Y-%m-%dT00:00:00.000Z")),
        ("plain dates, day -> next day",
         day.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")),
        ("plain dates, same day",
         day.strftime("%Y-%m-%d"), day.strftime("%Y-%m-%d")),
        ("midnight -> end of day (no ms)",
         day.strftime("%Y-%m-%dT00:00:00Z"), day.strftime("%Y-%m-%dT23:59:59Z")),
        ("no date filter at all", None, None),
    ]


async def _list_usage_files_for_day(session, headers, day, window_cache):
    """Tries the confirmed window format first once known; otherwise probes
    every candidate in order and remembers whichever one the API accepts,
    so a 31-day loop doesn't re-probe on every single day."""
    candidates = _usage_window_candidates(day)
    indices = [window_cache["index"]] if window_cache["index"] is not None else range(len(candidates))

    for idx in indices:
        label, f_val, t_val = candidates[idx]
        qs = []
        if f_val and t_val:
            qs = [f"fromDate={quote(f_val, safe='')}", f"toDate={quote(t_val, safe='')}"]
        list_url = f"{AERIS['analytics_base']}/usages"
        probe_url = URL(list_url + (f"?{'&'.join(qs)}" if qs else ""), encoded=True)

        async with session.get(probe_url, headers=headers, timeout=ClientTimeout(total=60)) as r:
            body = await r.text()
            if r.status != 200:
                if window_cache["index"] is None:
                    print(f"  usage window '{label}' rejected for {day} ({r.status}): {body[:160]}")
                continue
            try:
                candidate = json.loads(body)
            except json.JSONDecodeError:
                continue

        window_cache["index"] = idx
        return candidate.get("usages") or []

    return []


async def _download_usage_file(session, token, file_id, accept_cache):
    accepts = [accept_cache["accept"]] if accept_cache["accept"] else \
        ["*/*", "text/csv", "application/octet-stream", "application/gzip"]
    for accept in accepts:
        headers = {"Authorization": f"Bearer {token}", "Accept": accept}
        async with session.get(
            f"{AERIS['analytics_base']}/usages/{file_id}",
            headers=headers,
            timeout=ClientTimeout(total=120),
        ) as fr:
            if fr.status == 200:
                raw = await fr.read()
                accept_cache["accept"] = accept
                return raw
    return None


async def _fetch_day_records(session, headers, token, day, window_cache, accept_cache):
    files = await _list_usage_files_for_day(session, headers, day, window_cache)
    if not files:
        return []

    # A one-day window can return files stamped both <day> and <day+1>;
    # keep only file(s) whose listed date covers the target day so a
    # 31-day sum doesn't double count or pick up partial neighboring days.
    target_day = day.strftime("%Y-%m-%d")
    on_target = [f for f in files if str(f.get("date", "")).startswith(target_day)]
    files_to_dl = on_target or files

    records = []
    for f in files_to_dl:
        file_id = f.get("id")
        if not file_id:
            continue
        raw = await _download_usage_file(session, token, file_id, accept_cache)
        if raw is None:
            continue
        records.extend(_parse_usage_payload(raw, file_id))
    return records


async def fetch_usage_range(token, start_date, end_date, concurrency=USAGE_CONCURRENCY):
    days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    if not days:
        return pd.DataFrame(columns=["ICCID", "IMSI", "Telenor_MB_Usage"])

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    window_cache = {"index": None}
    accept_cache = {"accept": None}
    all_records = []

    async with aiohttp.ClientSession() as session:
        # Prime the window/accept-header caches sequentially on day one so
        # the concurrent batch below doesn't race on discovering them.
        all_records.extend(await _fetch_day_records(session, headers, token, days[0], window_cache, accept_cache))

        remaining = days[1:]
        if remaining:
            sem = asyncio.Semaphore(concurrency)

            async def _bounded(day):
                async with sem:
                    return await _fetch_day_records(session, headers, token, day, window_cache, accept_cache)

            tasks = [_bounded(d) for d in remaining]
            for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching Telenor usage files"):
                all_records.extend(await coro)

    if not all_records:
        print("⚠️ No Telenor usage records were returned for the window — usage will be 0 for every SIM.")
        return pd.DataFrame(columns=["ICCID", "IMSI", "Telenor_MB_Usage"])

    print(f"--- Sample usage record (1 of {len(all_records)}) ---")
    print(all_records[0])

    df = pd.DataFrame(all_records)
    iccid_col = _find_col(df, _USAGE_ICCID_CANDIDATES)
    imsi_col = _find_col(df, _USAGE_IMSI_CANDIDATES)
    qty_col = _find_col(df, _USAGE_QUANTITY_CANDIDATES)

    if not qty_col or not (iccid_col or imsi_col):
        print(f"⚠️ Couldn't confidently identify ICCID/IMSI + usage-volume columns "
              f"(iccid_col={iccid_col!r}, imsi_col={imsi_col!r}, qty_col={qty_col!r}) — "
              f"columns seen: {list(df.columns)}. Usage will be empty this run.")
        return pd.DataFrame(columns=["ICCID", "IMSI", "Telenor_MB_Usage"])

    out = pd.DataFrame()
    out["ICCID"] = df[iccid_col].astype(str).str.strip() if iccid_col else None
    out["IMSI"] = df[imsi_col].astype(str).str.strip() if imsi_col else None
    # UNCONFIRMED units — assuming bytes, converting to MB. If a sample
    # record shows the value is already KB or MB, change the divisor.
    out["Telenor_MB_Usage"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0) / (1024 * 1024)

    group_key = "ICCID" if iccid_col else "IMSI"
    result = out.groupby(group_key, as_index=False)["Telenor_MB_Usage"].sum()
    other_key = "IMSI" if group_key == "ICCID" else "ICCID"
    result[other_key] = None
    return result


# ==========================================================
# ZOHO ANALYTICS — the only source of Zenduit-side data for this report
# (ICCID -> AccountId/Device_Serial/Zenduit_Data_Plan/CompanyName/usage,
# plus AccountId -> customer name).
# ==========================================================
def get_zoho_analytics_token():
    _report_zoho_credential_sources()
    r = requests.post(
        f"https://{ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token",
        data={
            "refresh_token": ZOHO_ANALYTICS_REFRESH_TOKEN,
            "client_id": ZOHO_ANALYTICS_CLIENT_ID,
            "client_secret": ZOHO_ANALYTICS_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    data = r.json()
    if r.status_code != 200 or "access_token" not in data:
        raise RuntimeError(f"Zoho Analytics OAuth failed | status={r.status_code} | response={data}")

    granted = data.get("scope")
    print(f"Zoho token acquired. Granted scope: {granted or '(not reported by Zoho)'}")
    if granted and "ZohoAnalytics" not in granted:
        print("⚠️ The granted scope contains no ZohoAnalytics.* entries — exports below will "
              "fail with INVALID_OAUTHSCOPE. The refresh token needs Analytics scopes.")
    return data["access_token"]


def _zoho_headers(token):
    return {"Authorization": f"Zoho-oauthtoken {token}", "ZANALYTICS-ORGID": str(ZOHO_ORG_ID)}


def _zoho_create_job(token, view_id):
    url = f"{ZOHO_ANALYTICS_API}/bulk/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/views/{view_id}/data"
    params = {"CONFIG": json.dumps({"responseFormat": "csv"}, separators=(",", ":"))}
    resp = requests.get(url, params=params, headers=_zoho_headers(token), timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"Zoho Analytics create-export-job (view {view_id}) failed: "
                            f"{resp.status_code}: {resp.text[:500]}")
    job_id = (resp.json().get("data") or {}).get("jobId")
    if not job_id:
        raise RuntimeError(f"Zoho Analytics: no jobId returned for view {view_id}: {resp.text[:300]}")
    return job_id


def _zoho_wait(token, job_id, timeout_seconds=ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS):
    url = f"{ZOHO_ANALYTICS_API}/bulk/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/exportjobs/{job_id}"
    start = time.time()
    deadline = start + timeout_seconds
    last_log = start
    while time.time() < deadline:
        resp = requests.get(url, params={"responseFormat": "json"}, headers=_zoho_headers(token), timeout=60)
        resp.raise_for_status()
        info = resp.json().get("data") or {}
        if info.get("jobStatus") == "JOB COMPLETED" or str(info.get("jobCode")) == "1004":
            return info["downloadUrl"]
        if str(info.get("jobCode")) in ("1003", "1005"):
            raise RuntimeError(f"Zoho Analytics export job {job_id} failed: {info}")
        now = time.time()
        if now - last_log >= 30:
            print(f"  ...still waiting on Zoho Analytics export job {job_id} "
                  f"({now - start:.0f}s elapsed, timeout at {timeout_seconds}s)")
            last_log = now
        time.sleep(2)
    raise RuntimeError(f"Zoho Analytics export job {job_id} timed out after {timeout_seconds}s.")


def _zoho_download(token, url):
    resp = requests.get(url, headers={**_zoho_headers(token), "Accept-Encoding": "identity"}, timeout=180)
    resp.raise_for_status()
    text = resp.text
    return text[1:] if text and text[0] == "﻿" else text


def _flatten(obj, parent="", out=None):
    out = {} if out is None else out
    for k, v in obj.items():
        key = f"{parent}_{k}" if parent else k
        if isinstance(v, dict):
            _flatten(v, key, out)
        else:
            out[key] = v
    return out


def _parse_zoho_export(text):
    if text.lstrip()[:1] in ("{", "["):
        obj = json.loads(text)
        recs = obj.get("data") if isinstance(obj, dict) and "data" in obj else obj
        if isinstance(recs, dict):
            recs = [recs]
        return [_flatten(r) for r in recs if isinstance(r, dict)]
    return list(csv.DictReader(text.splitlines()))


def fetch_analytics_view(token, view_id, label, retries=2, timeout_seconds=ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS):
    """Runs a Zoho Analytics bulk export job and returns parsed rows (empty
    list on failure). Retries the whole job so a flaky export can't take
    down the run — the caller falls back to an empty lookup instead."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            job_id = _zoho_create_job(token, view_id)
            print(f"  Zoho Analytics export job created for '{label}': {job_id} (attempt {attempt}/{retries})")
            download_url = _zoho_wait(token, job_id, timeout_seconds=timeout_seconds)
            rows = _parse_zoho_export(_zoho_download(token, download_url))
            print(f"--- {label}: {len(rows)} rows. Columns found: "
                  f"{list(rows[0].keys()) if rows else '(no rows returned)'} ---")
            return rows
        except Exception as e:
            last_err = e
            print(f"⚠️ Zoho Analytics export for '{label}' failed on attempt {attempt}/{retries}: {e}")
    print(f"⚠️ Zoho Analytics export for '{label}' failed after {retries} attempt(s) — giving up. "
          f"Last error: {last_err}")
    return []


_ZENDUIT_DEVICE_COLUMNS = ["ICCID", "AccountId", "Device_Serial", "Zenduit_Data_Plan",
                           "CompanyName", "Zenduit_Usage_MB", "Zenduit_BillingStatus"]


def fetch_zenduit_devices_analytics(token):
    rows = fetch_analytics_view(token, ZOHO_ANALYTICS_ZENDUIT_DEVICES_VIEW_ID, "Zenduit Devices (analytics)")
    if not rows:
        print("⚠️ Zenduit Devices (analytics) export returned no data — "
              "device/account info will be empty for every SIM this run.")
        return pd.DataFrame(columns=_ZENDUIT_DEVICE_COLUMNS)

    df = pd.DataFrame(rows)
    col_map = {
        "ICCID": _find_col(df, ["ICCID", "SIM", "Sim", "SIM Number", "Iccid"]),
        "AccountId": _find_col(df, ["AccountId", "Account_Id", "AccountID", "Account Id",
                                     "CustomerId", "Customer_Id"]),
        "Device_Serial": _find_col(df, ["Device_Serial", "DeviceSerial", "Serial", "Device Serial"]),
        "Zenduit_Data_Plan": _find_col(df, ["Zenduit_Data_Plan", "DataPlan", "Data Plan",
                                             "Data_Plan", "Plan"]),
        "CompanyName": _find_col(df, ["CompanyName", "Company_Name", "Company Name", "Company"]),
        "Zenduit_Usage_MB": _find_col(df, ["Zenduit_Usage_MB", "Data_Usage", "Usage",
                                            "Zenduit_Usage", "Usage_MB", "Data Usage", "DataUsage"]),
        "Zenduit_BillingStatus": _find_col(df, ["Zenduit_BillingStatus", "BillingStatus",
                                                 "Billing_Status", "Billing Status", "Status"]),
    }
    print(f"Column match — Zenduit Devices (analytics): {col_map}")

    if not col_map["ICCID"]:
        print("⚠️ Couldn't find an ICCID/SIM column in the Zenduit Devices view — returning an empty lookup.")
        return pd.DataFrame(columns=_ZENDUIT_DEVICE_COLUMNS)

    missing = [k for k, v in col_map.items() if v is None and k != "ICCID"]
    if missing:
        print(f"⚠️ These fields weren't found and will be blank/0 for every SIM: {missing}.")

    out = pd.DataFrame()
    out["ICCID"] = df[col_map["ICCID"]].astype(str).str.strip()
    out["AccountId"] = df[col_map["AccountId"]].astype(str).str.strip() if col_map["AccountId"] else None
    out["Device_Serial"] = df[col_map["Device_Serial"]].astype(str).str.strip() if col_map["Device_Serial"] else None
    out["Zenduit_Data_Plan"] = (
        pd.to_numeric(df[col_map["Zenduit_Data_Plan"]], errors="coerce").fillna(0)
        if col_map["Zenduit_Data_Plan"] else 0
    )
    out["CompanyName"] = df[col_map["CompanyName"]] if col_map["CompanyName"] else None
    out["Zenduit_Usage_MB"] = (
        pd.to_numeric(df[col_map["Zenduit_Usage_MB"]], errors="coerce").fillna(0)
        if col_map["Zenduit_Usage_MB"] else 0
    )
    out["Zenduit_BillingStatus"] = df[col_map["Zenduit_BillingStatus"]] if col_map["Zenduit_BillingStatus"] else None

    total_rows, unique_iccids = len(out), out["ICCID"].nunique()
    if total_rows != unique_iccids:
        print(f"ℹ️ Zenduit Devices (analytics) has {total_rows} rows for only {unique_iccids} unique "
              f"ICCIDs — keeping the highest-Zenduit_Data_Plan row per ICCID.")
    out = out.sort_values("Zenduit_Data_Plan", ascending=False).drop_duplicates(subset=["ICCID"], keep="first")
    return out


def fetch_account_name_lookup(token):
    rows = fetch_analytics_view(token, ZOHO_ANALYTICS_ACCOUNTS_VIEW_ID, "Accounts (analytics)")
    if not rows:
        print("⚠️ Accounts (analytics) export returned no data — Analytics_Customer_Name will be empty.")
        return pd.DataFrame(columns=["AccountId", "Analytics_Customer_Name"])

    df_accounts = pd.DataFrame(rows)
    account_id_col = _find_col(df_accounts, ["Id", "AccountId", "Account_Id", "AccountID"])
    account_name_col = _find_col(df_accounts, ["Account Name", "AccountName", "Account_Name", "Name",
                                                "CustomerName", "Customer_Name"])
    print(f"Column match — accounts id: {account_id_col!r} | accounts name: {account_name_col!r}")
    if not (account_id_col and account_name_col):
        print("⚠️ Couldn't confidently identify the id/name columns in the Accounts view — "
              "returning an empty lookup.")
        return pd.DataFrame(columns=["AccountId", "Analytics_Customer_Name"])

    df_lookup = df_accounts[[account_id_col, account_name_col]].rename(
        columns={account_id_col: "AccountId", account_name_col: "Analytics_Customer_Name"}
    )
    df_lookup["AccountId"] = df_lookup["AccountId"].astype(str).str.strip()
    return df_lookup.drop_duplicates(subset=["AccountId"], keep="first")


# ==========================================================
# EMAIL — same shape as the 1NCE script's send_email().
# ==========================================================
def send_email(overconsumption_count, unmapped_count, excel_buffer):
    msg = EmailMessage()
    msg["Subject"] = "Monthly Telenor SIM Usage Audit – Overconsumption Report"
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_TO)
    msg.set_content(f"""
Hello Team,

Please find the monthly Telenor SIM usage audit report attached.

Summary:
- Overconsumption SIMs: {overconsumption_count}
- ICCIDs without active customer in Zenduit Devices (analytics): {unmapped_count}

Regards,
Nandhiv
""")
    msg.add_attachment(
        excel_buffer.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="telenor_overconsumption_report.xlsx",
    )
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
    print("📧 Email sent (Excel attached from memory)")


# ==========================================================
# MAIN
# ==========================================================
async def main():
    print("🔹 Getting Telenor (Aeris/IoT Accelerator) token...")
    token = await get_aeris_token()

    print("🔹 Fetching all Telenor subscribers...")
    df_base = await fetch_all_subscribers(token)

    today = datetime.now(timezone.utc).date()
    start_dt = today - timedelta(days=31)
    end_dt = today - timedelta(days=1)

    def zoho_analytics_chain():
        try:
            za_token = get_zoho_analytics_token()
            d_devices = fetch_zenduit_devices_analytics(za_token)
            d_account_name_lookup = fetch_account_name_lookup(za_token)
        except Exception as e:
            print(f"⚠️ Zoho Analytics lookups unavailable this run: {e}")
            d_devices = pd.DataFrame(columns=_ZENDUIT_DEVICE_COLUMNS)
            d_account_name_lookup = pd.DataFrame(columns=["AccountId", "Analytics_Customer_Name"])
        return d_devices, d_account_name_lookup

    print(f"🔹 Fetching Telenor usage ({start_dt} to {end_dt}) and Zoho Analytics data concurrently...")
    df_usage, (df_z_devices, df_account_name_lookup) = await asyncio.gather(
        fetch_usage_range(token, start_dt, end_dt),
        asyncio.to_thread(zoho_analytics_chain),
    )
    print(f"✅ Zenduit devices (analytics) fetched: {len(df_z_devices)}")

    # Join usage on ICCID where present, else fall back to IMSI.
    if df_usage["ICCID"].notna().any():
        df_base = df_base.merge(
            df_usage[["ICCID", "Telenor_MB_Usage"]].dropna(subset=["ICCID"]), on="ICCID", how="left"
        )
    else:
        df_base = df_base.merge(
            df_usage[["IMSI", "Telenor_MB_Usage"]].dropna(subset=["IMSI"]), on="IMSI", how="left"
        )

    # Step 1: Telenor (ICCID) <-> Zoho Analytics "Zenduit Devices" view.
    df_base = df_base.merge(df_z_devices, on="ICCID", how="left")
    df_base["Zenduit_Usage_MB"] = _numeric_fillna(df_base["Zenduit_Usage_MB"])

    # Step 2: AccountId -> Account Name via the Zoho Analytics Accounts table.
    df_base = df_base.merge(df_account_name_lookup, on="AccountId", how="left")
    print(f"✅ Account Name lookup merged: "
          f"{df_base['Analytics_Customer_Name'].notna().sum()} / {len(df_base)} SIMs matched")

    df_base["Telenor_MB_Usage"] = _numeric_fillna(df_base["Telenor_MB_Usage"])
    df_base["Zenduit_Data_Plan"] = _numeric_fillna(df_base["Zenduit_Data_Plan"])

    df_base["Consumption"] = ""
    df_base.loc[
        df_base["Telenor_MB_Usage"] > df_base["Zenduit_Data_Plan"],
        "Consumption"
    ] = "Overconsumption"

    device_iccids = set(df_z_devices["ICCID"].dropna().astype(str).str.strip())
    df_no_customer_in_zenduone = df_base[
        df_base["ICCID"].notna() &
        ~df_base["ICCID"].astype(str).str.strip().isin(device_iccids)
    ]

    df_account_summary = (
        df_base[df_base["AccountId"].notna()]
        .groupby(["AccountId", "Analytics_Customer_Name"], dropna=False)
        .agg(
            iccid_count=("ICCID", "nunique"),
            telenor_usage_mb=("Telenor_MB_Usage", "sum"),
            zenduit_usage_mb=("Zenduit_Usage_MB", "sum"),
        )
        .reset_index()
    )

    print("🔹 Writing Excel file...")
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df_base.to_excel(writer, sheet_name="base_combined", index=False)
        df_no_customer_in_zenduone.to_excel(writer, sheet_name="no_customer_in_zenduone", index=False)
        df_account_summary.to_excel(writer, sheet_name="account_usage_summary", index=False)
    excel_buffer.seek(0)
    print("✅ DONE → Excel generated")

    overconsumption_count = df_base[df_base["Consumption"] == "Overconsumption"].shape[0]
    unmapped_iccid_count = df_no_customer_in_zenduone.shape[0]
    print(f"📊 Overconsumption count: {overconsumption_count}")
    print(f"📊 ICCIDs without active customer: {unmapped_iccid_count}")

    print("🔹 Sending email...")
    send_email(
        overconsumption_count=overconsumption_count,
        unmapped_count=unmapped_iccid_count,
        excel_buffer=excel_buffer,
    )


if __name__ == "__main__":
    asyncio.run(main())
