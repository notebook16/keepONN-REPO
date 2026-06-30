#!/usr/bin/env python3
"""
keepONN daemon: continuously find IMEIs with bms_allow_discharging=false,
SET discharging_<imei> to status=pending (with cooldown via comman_log.csv).

Runs in a loop (default 60s). Deploy as systemd service via deploy.sh.
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

import redis

IST = ZoneInfo("Asia/Kolkata")
NUMERIC_KEY_PATTERN = re.compile(r"^[0-9]+$")

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_COMMAND_LOG = SCRIPT_DIR / "data" / "comman_log.csv"

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

CSV_COLUMNS = ("imei", "timestamp")
_shutdown_requested = False


def now_ist() -> datetime:
    return datetime.now(IST)


def format_ist(ts: datetime) -> str:
    return ts.isoformat()


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


def collect_discharging_off_imeis(client: redis.Redis) -> list[str]:
    off_imeis: list[str] = []
    for key_batch in chunked(iter_numeric_keys(client), PIPELINE_BATCH_SIZE):
        pipe = client.pipeline(transaction=False)
        for key in key_batch:
            pipe.get(key)
        values = pipe.execute()

        for key, raw in zip(key_batch, values):
            if extract_bms_allow_discharging(raw) is False:
                off_imeis.append(key)

    off_imeis.sort()
    return off_imeis


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


def load_command_log(path: Path) -> dict[str, datetime]:
    if not path.exists():
        return {}

    log: dict[str, datetime] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return log
        for row in reader:
            imei = (row.get("imei") or "").strip()
            ts_raw = (row.get("timestamp") or "").strip()
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


def apply_set(client: redis.Redis, imei: str) -> bool:
    redis_key = f"discharging_{imei}"
    device_type = resolve_device_type(client, imei)
    value = build_discharging_payload(imei, device_type)

    if DRY_RUN:
        print(f"  DRY-RUN {redis_key} value={value}")
        return True

    try:
        client.set(redis_key, value)
        print(f"  SET {redis_key} at {format_ist(now_ist())}")
        return True
    except redis.RedisError as exc:
        print(f"  FAILED {redis_key}: {exc}", file=sys.stderr)
        return False


def run_cycle(client: redis.Redis, command_log: dict[str, datetime]) -> dict[str, datetime]:
    cycle_start = now_ist()
    print(f"Cycle started at {format_ist(cycle_start)} (IST)")

    try:
        off_imeis = collect_discharging_off_imeis(client)
    except redis.RedisError as exc:
        print(f"Redis scan failed: {exc}", file=sys.stderr)
        return command_log

    eligible: list[str] = []
    skipped_cooldown = 0
    for imei in off_imeis:
        if is_eligible(imei, command_log, cycle_start):
            eligible.append(imei)
        else:
            skipped_cooldown += 1

    print(
        f"discharging_off={len(off_imeis)}, eligible={len(eligible)}, "
        f"skipped_cooldown={skipped_cooldown}"
    )

    success = 0
    failed = 0
    for index, imei in enumerate(eligible, start=1):
        if _shutdown_requested:
            break

        print(f"[{index}/{len(eligible)}] IMEI {imei}")
        if apply_set(client, imei):
            success += 1
            if not DRY_RUN:
                command_log[imei] = now_ist()
        else:
            failed += 1

        if not DRY_RUN and index < len(eligible) and SET_INTERVAL_SEC > 0:
            time.sleep(SET_INTERVAL_SEC)

    if not DRY_RUN and success > 0:
        save_command_log(COMMAND_LOG_CSV, command_log)

    label = "would set" if DRY_RUN else "set"
    print(
        f"Cycle finished at {format_ist(now_ist())} (IST): "
        f"{label}={success}, failed={failed}"
    )
    return command_log


def main() -> int:
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    print(f"keepONN starting — mode={mode}")
    print(f"Command log: {COMMAND_LOG_CSV}")
    print(f"Cooldown: {COOLDOWN_MINUTES} min, loop interval: {LOOP_INTERVAL_SEC}s")

    command_log = load_command_log(COMMAND_LOG_CSV)

    while not _shutdown_requested:
        client: redis.Redis | None = None
        try:
            client = connect_redis()
            client.ping()
            command_log = run_cycle(client, command_log)
        except redis.RedisError as exc:
            print(f"Redis connection failed: {exc}", file=sys.stderr)
        finally:
            if client is not None:
                client.close()

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
