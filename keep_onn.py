#!/usr/bin/env python3
"""
keepONN daemon:
1) Scan Redis numeric IMEI keys where bms_allow_discharging=false
2) Filter out IMEIs present in override CSV
3) Filter out IMEIs absent in fleets.io_t_device_imei
4) Filter out IMEIs whose latest command_requests row is discharging + command_status=false
   with created_at newer than configured cutoff
5) Apply cooldown via comman_log.csv and SET discharging_<imei> status=pending
"""

from __future__ import annotations

import csv
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import psycopg
import redis

IST = ZoneInfo("Asia/Kolkata")
NUMERIC_KEY_PATTERN = re.compile(r"^[0-9]+$")

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_COMMAND_LOG = SCRIPT_DIR / "data" / "comman_log.csv"
DEFAULT_OVERRIDE_CSV = SCRIPT_DIR / "data" / "override.csv"
DEFAULT_FIRED_COMMAND_CSV = SCRIPT_DIR / "data" / "fired_command.csv"

REDIS_HOST = os.environ.get("REDIS_HOST", "65.0.205.125")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASS = os.environ.get("REDIS_PASS", "HeyEV@123Rdb")
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
SOCKET_TIMEOUT_SEC = float(os.environ.get("REDIS_SOCKET_TIMEOUT_SEC", "30"))
SCAN_COUNT = int(os.environ.get("SCAN_COUNT", "1000"))
PIPELINE_BATCH_SIZE = int(os.environ.get("PIPELINE_BATCH_SIZE", "500"))
COOLDOWN_MINUTES = float(os.environ.get("COOLDOWN_MINUTES", "10"))
LOOP_INTERVAL_SEC = float(os.environ.get("LOOP_INTERVAL_SEC", "60"))
SET_INTERVAL_SEC = float(os.environ.get("SET_INTERVAL_SEC", "0.10"))
DEFAULT_DEVICE_TYPE = os.environ.get("DEFAULT_DEVICE_TYPE", "Teltonika-TFT100")
PLATFORM = os.environ.get("PLATFORM", "heyev")
DRY_RUN = os.environ.get("DRY_RUN", "").strip().lower() in {"1", "true", "yes"}

COMMAND_LOG_CSV = Path(
    os.environ.get("COMMAND_LOG_CSV", "").strip() or DEFAULT_COMMAND_LOG
)
OVERRIDE_CSV = Path(os.environ.get("OVERRIDE_CSV", "").strip() or DEFAULT_OVERRIDE_CSV)
FIRED_COMMAND_CSV = Path(
    os.environ.get("FIRED_COMMAND_CSV", "").strip() or DEFAULT_FIRED_COMMAND_CSV
)

DB_POSTGRES_URL = os.environ.get("DB_POSTGRES_URL", "127.0.0.1")
DB_POSTGRES_DBNAME = os.environ.get("DB_POSTGRES_DBNAME", "ac2")
DB_POSTGRES_USERNAME = os.environ.get("DB_POSTGRES_USERNAME", "postgres")
DB_POSTGRES_PASS = os.environ.get("DB_POSTGRES_PASS", "")
DB_POSTGRES_PORT = int(os.environ.get("DB_POSTGRES_PORT", "5432"))
DB_CONNECT_TIMEOUT_SEC = int(os.environ.get("DB_CONNECT_TIMEOUT_SEC", "10"))
COMMAND_REQUEST_MIN_CREATED_AT_RAW = os.environ.get(
    "COMMAND_REQUEST_MIN_CREATED_AT", "2026-06-30T18:29:59Z"
)

CSV_COLUMNS = ("imei", "timestamp")
FIRED_COMMAND_COLUMNS = ("imei", "timestamp")
_shutdown_requested = False


def now_ist() -> datetime:
    return datetime.now(IST)


def format_ist(ts: datetime) -> str:
    return ts.isoformat()


def normalize_imei(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def request_shutdown(signum: int, _frame: Any) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    print(f"Shutdown requested (signal {signum}); finishing current work...", file=sys.stderr)


def connect_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASS,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_SEC,
        socket_connect_timeout=SOCKET_TIMEOUT_SEC,
    )


def connect_postgres() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_POSTGRES_URL,
        dbname=DB_POSTGRES_DBNAME,
        user=DB_POSTGRES_USERNAME,
        password=DB_POSTGRES_PASS,
        port=DB_POSTGRES_PORT,
        connect_timeout=DB_CONNECT_TIMEOUT_SEC,
    )


def iter_numeric_keys(client: redis.Redis) -> Iterator[str]:
    """Yield keys matching ^[0-9]+$ using SCAN (never KEYS)."""
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor=cursor, count=SCAN_COUNT)
        for key in keys:
            if NUMERIC_KEY_PATTERN.fullmatch(key):
                yield key
        if cursor == 0:
            break


def chunked(items: Iterator[str], size: int) -> Iterator[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def extract_bms_allow_discharging(raw: str | None) -> bool | None:
    if not raw:
        return None
    try:
        payload: Any = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

    boolean_io = payload.get("boolean_io_data")
    if not isinstance(boolean_io, dict):
        return None

    value = boolean_io.get("bms_allow_discharging")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
    return None


def extract_soc(raw: str | None) -> float | None:
    if not raw:
        return None
    try:
        payload: Any = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

    numeric_io = payload.get("numeric_io_data")
    if not isinstance(numeric_io, dict):
        return None

    value = numeric_io.get("soc")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        try:
            return float(trimmed)
        except ValueError:
            return None
    return None


def collect_discharging_off_imeis(client: redis.Redis) -> tuple[list[str], list[str]]:
    off_imeis: list[str] = []
    skipped_soc_zero_imeis: list[str] = []
    for key_batch in chunked(iter_numeric_keys(client), PIPELINE_BATCH_SIZE):
        pipe = client.pipeline(transaction=False)
        for key in key_batch:
            pipe.get(key)
        values = pipe.execute()

        for key, raw in zip(key_batch, values):
            if extract_bms_allow_discharging(raw) is not False:
                continue
            soc = extract_soc(raw)
            if soc == 0:
                skipped_soc_zero_imeis.append(key)
            else:
                off_imeis.append(key)

    off_imeis.sort()
    skipped_soc_zero_imeis.sort()
    return off_imeis, skipped_soc_zero_imeis


def parse_csv_timestamp(raw: str) -> datetime | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def parse_min_created_at(raw: str) -> datetime:
    ts = parse_csv_timestamp(raw)
    if ts is None:
        raise ValueError(
            "Invalid COMMAND_REQUEST_MIN_CREATED_AT. Use ISO format like "
            "2026-06-30T18:29:59Z"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts


def load_command_log(path: Path) -> dict[str, datetime]:
    if not path.exists():
        return {}

    log: dict[str, datetime] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return log
        for row in reader:
            imei = normalize_imei(row.get("imei"))
            ts_raw = normalize_imei(row.get("timestamp"))
            if not imei:
                continue
            ts = parse_csv_timestamp(ts_raw)
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            log[imei] = ts
    return log


def save_command_log(path: Path, log: dict[str, datetime]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_COLUMNS)
        for imei in sorted(log):
            writer.writerow([imei, format_ist(log[imei])])

    tmp_path.replace(path)


def append_fired_command_rows(path: Path, rows: list[tuple[str, str]]) -> None:
    """Append rows to fired command CSV: (imei, set_timestamp_ist)."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if not file_exists or path.stat().st_size == 0:
            writer.writerow(FIRED_COMMAND_COLUMNS)
        writer.writerows(rows)


def load_override_imeis(path: Path) -> set[str]:
    if not path.exists():
        return set()

    override_imeis: set[str] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return override_imeis
        for row in reader:
            imei = normalize_imei(row.get("imei"))
            if imei:
                override_imeis.add(imei)
    return override_imeis


def log_filtered_imeis(label: str, imeis: list[str]) -> None:
    if not imeis:
        return
    print(f"{label}_imeis={','.join(imeis)}", flush=True)


def log_override_entries(override_imeis: set[str]) -> None:
    if not override_imeis:
        print("override_csv_entries=none", flush=True)
        return
    sorted_imeis = sorted(override_imeis)
    print(f"override_csv_entries_count={len(sorted_imeis)}", flush=True)
    print(f"override_csv_entries_imeis={','.join(sorted_imeis)}", flush=True)


def fetch_fleet_imeis(pg_conn: psycopg.Connection) -> set[str]:
    query = """
        SELECT DISTINCT io_t_device_imei
        FROM fleets
        WHERE io_t_device_imei IS NOT NULL
          AND BTRIM(io_t_device_imei) <> '';
    """
    with pg_conn.cursor() as cur:
        cur.execute(query)
        return {normalize_imei(row[0]) for row in cur.fetchall() if normalize_imei(row[0])}


def fetch_latest_discharging_false_imeis(
    pg_conn: psycopg.Connection,
    imeis: list[str],
    min_created_at: datetime,
) -> set[str]:
    if not imeis:
        return set()

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (imei)
                imei,
                command,
                command_status,
                created_at
            FROM command_requests
            WHERE imei = ANY(%s)
            ORDER BY imei, created_at DESC NULLS LAST, id DESC
        )
        SELECT imei
        FROM latest
        WHERE LOWER(command) = 'discharging'
          AND command_status = FALSE
          AND created_at IS NOT NULL
          AND created_at > %s;
    """
    with pg_conn.cursor() as cur:
        cur.execute(query, (imeis, min_created_at))
        return {normalize_imei(row[0]) for row in cur.fetchall() if normalize_imei(row[0])}


def is_eligible(imei: str, log: dict[str, datetime], now: datetime) -> bool:
    last_ts = log.get(imei)
    if last_ts is None:
        return True
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=IST)
    return now - last_ts >= timedelta(minutes=COOLDOWN_MINUTES)


def utc_timestamp() -> str:
    now = datetime.now(timezone.utc)
    nano_frac = f"{now.microsecond * 1000:09d}"
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + nano_frac + "Z"


def build_discharging_payload(imei: str, device_type: str) -> str:
    ts = utc_timestamp()
    payload: dict[str, Any] = {
        "device_type": device_type,
        "deviceid": imei,
        "message": "true",
        "platform": PLATFORM,
        "status": "pending",
        "timestamp": ts,
        "type": "discharging",
        "updated_at": ts,
    }
    return json.dumps(payload, separators=(",", ":"))


def resolve_device_type(client: redis.Redis, imei: str) -> str:
    existing = client.get(f"discharging_{imei}")
    if not existing:
        return DEFAULT_DEVICE_TYPE
    try:
        payload: Any = json.loads(existing)
    except (json.JSONDecodeError, TypeError):
        return DEFAULT_DEVICE_TYPE
    value = payload.get("device_type")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return DEFAULT_DEVICE_TYPE


def apply_set(client: redis.Redis, imei: str) -> tuple[bool, str]:
    redis_key = f"discharging_{imei}"
    device_type = resolve_device_type(client, imei)
    value = build_discharging_payload(imei, device_type)

    if DRY_RUN:
        print(f"  DRY-RUN {redis_key} value={value}")
        return True, ""

    try:
        set_ts = format_ist(now_ist())
        client.set(redis_key, value)
        print(f"  SET {redis_key} at {set_ts}")
        return True, set_ts
    except redis.RedisError as exc:
        print(f"  FAILED {redis_key}: {exc}", file=sys.stderr)
        return False, ""


def run_cycle(
    redis_client: redis.Redis,
    pg_conn: psycopg.Connection,
    command_log: dict[str, datetime],
    min_created_at: datetime,
) -> dict[str, datetime]:
    cycle_start = now_ist()
    print(f"Cycle started at {format_ist(cycle_start)} (IST)", flush=True)

    try:
        off_imeis, skipped_soc_zero_imeis = collect_discharging_off_imeis(redis_client)
    except redis.RedisError as exc:
        print(f"Redis scan failed: {exc}", file=sys.stderr)
        return command_log

    override_imeis = load_override_imeis(OVERRIDE_CSV)
    log_override_entries(override_imeis)
    fleet_imeis = fetch_fleet_imeis(pg_conn)

    override_filtered = [imei for imei in off_imeis if imei not in override_imeis]
    skipped_override = len(off_imeis) - len(override_filtered)

    fleets_filtered = [imei for imei in override_filtered if imei in fleet_imeis]
    skipped_absent_fleets_imeis = [
        imei for imei in override_filtered if imei not in fleet_imeis
    ]
    skipped_absent_fleets = len(override_filtered) - len(fleets_filtered)

    latest_discharging_false = fetch_latest_discharging_false_imeis(
        pg_conn, fleets_filtered, min_created_at
    )
    command_filtered = [imei for imei in fleets_filtered if imei not in latest_discharging_false]
    skipped_latest_false_imeis = sorted(latest_discharging_false)
    skipped_latest_false = len(fleets_filtered) - len(command_filtered)

    eligible: list[str] = []
    skipped_cooldown = 0
    for imei in command_filtered:
        if is_eligible(imei, command_log, cycle_start):
            eligible.append(imei)
        else:
            skipped_cooldown += 1

    redis_discharging_off_total = len(off_imeis) + len(skipped_soc_zero_imeis)
    after_soc_filter = len(off_imeis)

    print(
        "redis_discharging_off_total="
        f"{redis_discharging_off_total}, "
        f"skipped_soc_zero={len(skipped_soc_zero_imeis)}, "
        f"after_soc_filter={after_soc_filter}, "
        f"skipped_override={skipped_override}, "
        f"skipped_absent_fleets={skipped_absent_fleets}, "
        f"skipped_latest_discharging_false={skipped_latest_false}, "
        f"eligible={len(eligible)}, skipped_cooldown={skipped_cooldown}",
        flush=True,
    )
    log_filtered_imeis("skipped_soc_zero", skipped_soc_zero_imeis)
    log_filtered_imeis("skipped_absent_fleets", skipped_absent_fleets_imeis)
    log_filtered_imeis("skipped_latest_discharging_false", skipped_latest_false_imeis)

    success = 0
    failed = 0
    fired_command_rows: list[tuple[str, str]] = []
    for index, imei in enumerate(eligible, start=1):
        if _shutdown_requested:
            break

        print(f"[{index}/{len(eligible)}] IMEI {imei}")
        ok, set_ts = apply_set(redis_client, imei)
        if ok:
            success += 1
            if not DRY_RUN:
                command_log[imei] = now_ist()
                fired_command_rows.append((imei, set_ts))
        else:
            failed += 1

        if not DRY_RUN and index < len(eligible) and SET_INTERVAL_SEC > 0:
            time.sleep(SET_INTERVAL_SEC)

    if not DRY_RUN and success > 0:
        save_command_log(COMMAND_LOG_CSV, command_log)
        append_fired_command_rows(FIRED_COMMAND_CSV, fired_command_rows)

    label = "would set" if DRY_RUN else "set"
    print(
        f"Cycle finished at {format_ist(now_ist())} (IST): "
        f"{label}={success}, failed={failed}",
        flush=True,
    )
    return command_log


def main() -> int:
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    try:
        min_created_at = parse_min_created_at(COMMAND_REQUEST_MIN_CREATED_AT_RAW)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    print(f"keepONN starting — mode={mode}", flush=True)
    print(f"Command log: {COMMAND_LOG_CSV}", flush=True)
    print(f"Override CSV: {OVERRIDE_CSV}", flush=True)
    print(f"Command request lower bound: {min_created_at.isoformat()}", flush=True)
    print(
        f"Cooldown: {COOLDOWN_MINUTES} min, loop interval: {LOOP_INTERVAL_SEC}s",
        flush=True,
    )

    command_log = load_command_log(COMMAND_LOG_CSV)

    while not _shutdown_requested:
        redis_client: redis.Redis | None = None
        pg_conn: psycopg.Connection | None = None
        try:
            print(
                f"Connecting Redis at {REDIS_HOST}:{REDIS_PORT} db={REDIS_DB}...",
                flush=True,
            )
            redis_client = connect_redis()
            redis_client.ping()
            print("Redis connection established.", flush=True)

            print(
                f"Connecting Postgres at {DB_POSTGRES_URL}:{DB_POSTGRES_PORT} "
                f"db={DB_POSTGRES_DBNAME} user={DB_POSTGRES_USERNAME}...",
                flush=True,
            )
            pg_conn = connect_postgres()
            print("Postgres connection established.", flush=True)

            command_log = run_cycle(redis_client, pg_conn, command_log, min_created_at)
        except redis.RedisError as exc:
            print(f"Redis connection failed: {exc}", file=sys.stderr)
        except psycopg.Error as exc:
            print(f"Postgres error: {exc}", file=sys.stderr)
        finally:
            if redis_client is not None:
                redis_client.close()
            if pg_conn is not None:
                pg_conn.close()

        if _shutdown_requested:
            break

        slept = 0.0
        while slept < LOOP_INTERVAL_SEC and not _shutdown_requested:
            chunk = min(1.0, LOOP_INTERVAL_SEC - slept)
            time.sleep(chunk)
            slept += chunk

    if not DRY_RUN and command_log:
        save_command_log(COMMAND_LOG_CSV, command_log)

    print("keepONN stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
