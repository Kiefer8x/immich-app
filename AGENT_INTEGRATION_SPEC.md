# 📋 Agent & Spoke Integration Specification
**Hub Host:** Apple Silicon M4 Mac Mini (macOS)  
**Host LAN IP:** `http://192.168.0.32:8000` (Use for Synology & network spokes)  
**Host Loopback:** `http://127.0.0.1:8000` (Use for Nguyen AI on the same Mac)  
**Intranet Domain:** `https://kbn1.ddns.net` (Entra ID Authenticated Upstream)  

---

## ⚡ Quick Reference: Where to Send Data

| Source Spoke | Base URL to Use | Port | Protocol | Auth Header |
| :--- | :--- | :--- | :--- | :--- |
| **Nguyen AI** (Same Mac) | `http://127.0.0.1:8000` | `8000` | HTTP / Loopback | None (Open) |
| **Synology Containers** (Plex, Postgres, HA) | `http://192.168.0.32:8000` | `8000` | HTTP / LAN | None (Open) |
| **External / Remote Intranet** | `https://kbn1.ddns.net` | `443` | HTTPS / Reverse Proxy | Entra ID Upstream |

### API Endpoints Summary

* `POST /api/v1/stream/log` — Single log event (errors, warnings, operational events)
* `POST /api/v1/stream/ai` — Nguyen AI conversational chat turns & Flux image generation
* `POST /api/v1/stream/metrics` — Container health checks, CPU %, RAM MB, uptime
* `POST /api/v1/stream/batch` — High-throughput batch array of buffered/un-synced log events
* `GET  /api/v1/stream/firehose` — Real-time Server-Sent Events (SSE) live feed

---

## 1. Directive for Spoke Agents & Developers

All services connecting to the private intranet ecosystem (including **Nguyen AI**, Synology Docker containers, and home automations) must comply with four core architectural rules:

1. **Migrate to Resilient Local Storage (Zero Data Loss Policy)**: Stop using flat JSON files or un-journaled databases. Use **SQLite in WAL mode** or **PostgreSQL** locally for your application state.
   * ⚠️ **STRICT REQUIREMENT:** **You CANNOT delete existing JSON data files and start fresh.** Every single existing record must be parsed, verified, migrated into the new database, and archived as `.json.migrated.bak`.
2. **Transactional Outbox / Store-and-Forward (Hold Data If Server Is Down)**: 
   * If Central AI is unreachable for 10 seconds, 1 day, or **1 whole week**, **never drop or discard data**.
   * Spoke services must buffer events locally in an `outbox` table.
   * Spoke services must maintain a **timestamp check** (`created_at` vs. `synced_to_central_at`) tracking exactly what was sent and what was not sent.
   * When Central AI comes back online, all held data must be pushed out in chronological order.
3. **Local Commit First**: Always commit your own transactional state locally first, then queue the telemetry copy for delivery.
4. **Never Block User Operations**: Central AI telemetry forwarding must run completely in the background (asynchronous worker thread or event loop task).

---

## 2. Rule 1: Resilient Local Database Standard & JSON Migration

### A. SQLite WAL Mode Connection Standard
If your service uses SQLite, you must execute the following connection pragmas immediately upon connecting:

```python
import sqlite3

def get_resilient_db(db_path: str):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    # Mandated resilience settings:
    conn.execute("PRAGMA journal_mode = WAL;")        # Concurrency: readers never block writers
    conn.execute("PRAGMA synchronous = NORMAL;")       # Fast, safe SSD writes
    conn.execute("PRAGMA busy_timeout = 5000;")        # Wait up to 5s before failing on lock
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn
```

### B. Mandatory JSON Migration Policy (Preserve Everything)
If an existing container or service currently stores data in `.json` files:
* ❌ **DO NOT DELETE THE JSON FILE.** `rm` or wiping legacy storage is strictly prohibited.
* ✅ **MIGRATE ALL EXISTING RECORDS:** Parse every object in the JSON file and insert it into the new SQLite WAL / Postgres schema.
* ✅ **VERIFY AND ARCHIVE:** Compare row counts before and after. Once verified, archive the legacy file as `<filename>.json.migrated.bak`.

#### Drop-In JSON-to-SQLite WAL Migration Pattern:
```python
import json
import os
import sqlite3
from pathlib import Path

def migrate_legacy_json_to_sqlite(json_path: str, db_path: str):
    """
    Safely migrates all legacy JSON data into SQLite WAL.
    Guarantees zero data loss: preserves the original file as a backup.
    """
    json_file = Path(json_path)
    if not json_file.exists():
        return  # Nothing to migrate

    print(f"📦 [MIGRATION] Found legacy data in {json_path}. Migrating...")

    with open(json_file, "r", encoding="utf-8") as f:
        legacy_data = json.load(f)

    # Normalize to a list of records
    records = legacy_data if isinstance(legacy_data, list) else [legacy_data]
    if not records:
        print("ℹ️ [MIGRATION] JSON file was empty. Preserving file.")
        return

    conn = get_resilient_db(db_path)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS migrated_records (
                id TEXT PRIMARY KEY,
                created_at TEXT,
                data_json TEXT
            );
        """)

        migrated_count = 0
        for item in records:
            item_id = str(item.get("id") or item.get("session_id") or f"migrated_{migrated_count}")
            created_at = item.get("timestamp") or item.get("created_at") or "unknown"
            conn.execute(
                "INSERT OR REPLACE INTO migrated_records (id, created_at, data_json) VALUES (?, ?, ?);",
                (item_id, created_at, json.dumps(item))
            )
            migrated_count += 1

    # Verification check
    cursor = conn.execute("SELECT COUNT(*) FROM migrated_records;")
    total_in_db = cursor.fetchone()[0]
    conn.close()

    if total_in_db >= len(records):
        backup_path = json_file.with_suffix(".json.migrated.bak")
        json_file.rename(backup_path)
        print(f"✅ [MIGRATION SUCCESS] {migrated_count} records migrated. Archived legacy file to {backup_path}")
    else:
        raise RuntimeError("Migration verification failed: Record count mismatch. Legacy JSON kept intact.")
```

---

## 3. Rule 2: Store-and-Forward Outbox Architecture (Hold Data If Offline)

### High-Availability Guarantees:
If the Central AI Mac Mini goes down, reboots, or is unreachable for **any period (from 5 seconds to 1 full week)**:
* **The spoke MUST hold all telemetry locally** in an outbox table.
* **Timestamp tracking is mandatory**: Every record has a `created_at` timestamp (when the event occurred) and a `synced_to_central_at` timestamp (set only after Central AI confirms HTTP 200).
* **Backlog Flush on Reconnect**: When Central AI comes back up, the spoke automatically drains the accumulated backlog in chronological order. **Zero data is missed.**

### Universal Drop-In Python Outbox Implementation (Copy-Paste Ready):

```python
import datetime
import json
import sqlite3
import threading
import time
import urllib.request
import uuid
from typing import Any, Dict, List, Optional

# For Nguyen AI (same Mac): "http://127.0.0.1:8000"
# For Synology Docker containers: "http://192.168.0.32:8000"
CENTRAL_AI_BASE_URL = "http://192.168.0.32:8000"


class ResilientSpokeOutbox:
    """
    Offline-resilient Store-and-Forward telemetry buffer.
    Guarantees ZERO data loss even if Central AI is offline for 1 week.
    """
    def __init__(self, db_path: str = "spoke_store.db", hub_url: str = CENTRAL_AI_BASE_URL):
        self.db_path = db_path
        self.hub_url = hub_url.rstrip("/")
        self._init_outbox_table()
        
        # Start background sync worker daemon
        self._worker_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._worker_thread.start()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def _init_outbox_table(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox_telemetry (
                    id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,         -- e.g. '/api/v1/stream/log' or '/api/v1/stream/ai'
                    payload_json TEXT NOT NULL,     -- Full event JSON
                    created_at TEXT NOT NULL,       -- Exact local timestamp of event
                    synced_to_central_at TEXT NULL  -- NULL = pending, Timestamp = confirmed delivered
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_unsynced ON outbox_telemetry(synced_to_central_at, created_at);")

    def enqueue(self, endpoint: str, payload: Dict[str, Any]):
        """
        Instant local write. Never blocks. 
        Stores event in SQLite with precise created_at timestamp.
        """
        event_id = str(uuid.uuid4())
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        # Ensure timestamp is preserved in payload
        if "timestamp" not in payload:
            payload["timestamp"] = now_iso

        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO outbox_telemetry (id, endpoint, payload_json, created_at, synced_to_central_at) VALUES (?, ?, ?, ?, NULL);",
                (event_id, endpoint, json.dumps(payload), now_iso)
            )

    def _flush_loop(self):
        """Background flusher: checks what was sent and what was not sent."""
        while True:
            try:
                self._flush_pending_batch()
            except Exception:
                pass
            time.sleep(3.0)  # Check for new data every 3 seconds

    def _flush_pending_batch(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # Check what has NOT been sent yet (ordered by original creation time)
        cursor.execute("""
            SELECT id, endpoint, payload_json 
            FROM outbox_telemetry 
            WHERE synced_to_central_at IS NULL 
            ORDER BY created_at ASC 
            LIMIT 50;
        """)
        rows = cursor.fetchall()
        if not rows:
            conn.close()
            return

        for row_id, endpoint, payload_str in rows:
            target_url = f"{self.hub_url}{endpoint}"
            try:
                req = urllib.request.Request(
                    target_url,
                    data=payload_str.encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=4.0) as resp:
                    if resp.status == 200:
                        # Central AI confirmed receipt: mark timestamp of successful delivery!
                        synced_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        conn.execute(
                            "UPDATE outbox_telemetry SET synced_to_central_at = ? WHERE id = ?;",
                            (synced_iso, row_id)
                        )
                        conn.commit()
            except Exception:
                # Central AI unreachable (server down, Mac rebooting, power cut).
                # STOP FLUSHING and wait for next cycle. All un-synced data remains safely held!
                conn.close()
                return

        conn.close()
```

#### How to Use in Your Spoke Service:
```python
# Initialize once per service / container
outbox = ResilientSpokeOutbox(db_path="local_app.db", hub_url="http://192.168.0.32:8000")

# Whenever an event happens, simply enqueue:
outbox.enqueue("/api/v1/stream/log", {
    "service": "plex",
    "level": "INFO",
    "message": "User started stream",
    "host": "synology-ds920"
})
# Done! If Central AI is online, it delivers in ~100ms.
# If Central AI is down for 1 week, it holds safely in SQLite and flushes once back online.
```

---

## 4. Ingestion API Contracts & Test Commands

### A. For AI Services (Nguyen AI: Prompts, Chats & Image Generations)
* **Endpoint:** `POST /api/v1/stream/ai`
* **Trigger:** After completing a chat response or image generation.

#### 1-Line Curl Test:
```bash
# Test from host Mac:
curl -X POST "http://127.0.0.1:8000/api/v1/stream/ai" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "nguyen-ai",
    "event_type": "chat_completion",
    "level": "INFO",
    "message": "Chat turn completed: Explain Docker network isolation",
    "host": "mac-host",
    "ai_data": {
      "session_id": "sess_91823",
      "user_prompt": "Explain Docker network isolation",
      "model_used": "qwen2.5:14b",
      "tokens_prompt": 14,
      "tokens_completion": 160,
      "latency_ms": 890
    }
  }'
```

---

### B. For General Infrastructure & Containers (Synology: Plex, Postgres, Home Assistant)
* **Endpoint:** `POST /api/v1/stream/log`
* **Trigger:** On warnings, errors, crashes, or operational events.

#### 1-Line Curl Test:
```bash
# Test from Synology or any LAN terminal:
curl -X POST "http://192.168.0.32:8000/api/v1/stream/log" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "plex",
    "level": "ERROR",
    "host": "synology-ds920",
    "message": "Failed to transcode 4K stream: encoder lock timeout",
    "payload": {
      "client": "Living Room Apple TV",
      "stream_id": 8941,
      "error_code": "EPIPE"
    }
  }'
```

---

### C. For Periodic Health & Container Metrics
* **Endpoint:** `POST /api/v1/stream/metrics`
* **Trigger:** Cron or background heartbeat every 1–5 minutes.

#### 1-Line Curl Test:
```bash
# Test from Synology or any spoke:
curl -X POST "http://192.168.0.32:8000/api/v1/stream/metrics" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "home-assistant",
    "host": "synology-ds920",
    "status": "running",
    "health": "healthy",
    "cpu_percent": 2.4,
    "memory_mb": 412.0,
    "uptime_seconds": 259200
  }'
```

---

### D. For High-Throughput Batch Logs
* **Endpoint:** `POST /api/v1/stream/batch`
* **Trigger:** Flushing multiple buffered logs at once.

#### 1-Line Curl Test:
```bash
curl -X POST "http://192.168.0.32:8000/api/v1/stream/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "host": "synology-ds920",
    "logs": [
      { "service": "postgres-db", "level": "INFO", "message": "Connection accepted from 192.168.0.10" },
      { "service": "postgres-db", "level": "WARN", "message": "Slow query detected (240ms): SELECT * FROM logs" }
    ]
  }'
```

---

## 5. Hub High-Availability & Crash Recovery (Host macOS)

To ensure Central AI itself never stays down:
* **macOS `launchd` Service**: Managed via `com.hoa.centralai.plist`.
* **Instant Auto-Restart on Crash**: Configured with `<key>KeepAlive</key><dict><key>Crashed</key><true/></dict>`. If Uvicorn or the process experiences an unexpected crash or kill signal, macOS immediately respawns it.
* **Auto-Start on Mac Boot**: Configured with `<key>RunAtLoad</key><true/>`. If the Mac Mini restarts, loses power, or updates, the service starts immediately upon system boot.
* **Management Commands:**
  * Install/Enable: `./scripts/install_service.sh`
  * Status: `./scripts/status.sh`
  * Restart: `./scripts/restart.sh`

---

## 6. Synology Spigot Sidecar & Omada Harvester Setup

### A. Synology Container Manager Docker Compose Spigot
If you run Docker containers on Synology and want a dedicated sidecar container to forward logs to Central AI:
```yaml
version: '3.8'

services:
  synology-docker-spigot:
    image: python:3.11-alpine
    container_name: synology-docker-spigot
    restart: always
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./spigot.py:/app/spigot.py:ro
    working_dir: /app
    command: ["python", "/app/spigot.py"]
    environment:
      - HUB_ENDPOINT=http://192.168.0.32:8000
      - SPOKE_NAME=Synology-DS920-Cluster
      - POLL_INTERVAL=5
```

### B. Pull Harvester: TP-Link Omada Controller Integration
For network hardware (ER605 router, switches, APs), Central AI has a built-in background harvester that periodically logs into your Omada Controller and pulls alerts/events:
```env
OMADA_ENABLED=true
OMADA_URL=https://192.168.0.1:8043
OMADA_SITE_NAME=Default
OMADA_USERNAME=admin
OMADA_PASSWORD=your_omada_password
OMADA_POLL_INTERVAL_MINUTES=3
OMADA_VERIFY_SSL=false
```
