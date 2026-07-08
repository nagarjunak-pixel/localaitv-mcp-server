#!/usr/bin/env python3
"""
LocalAI TV Admin MCP Server
Gives Claude direct access to:
  - newsai_db (GCP CloudSQL PostgreSQL 18)
  - AWS ECS / S3 / SQS / CloudWatch (ap-south-1 compute, ap-south-2 S3)
  - LocalAI TV REST API (https://localaitv.com)

Run:  python server.py
Add to ~/.claude/claude_desktop_config.json (see README.md)
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any
from pathlib import Path

# Load .env from same directory as this script (keeps DB password out of claude_desktop_config.json)
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

import psycopg2
import psycopg2.extras
import boto3
import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

# ── Config ──────────────────────────────────────────────────────────────────
DATABASE_URL     = os.environ.get("DATABASE_URL", "")           # postgres://user:pw@host/newsai_db
AWS_REGION_COMP  = os.environ.get("AWS_REGION_COMP", "ap-south-1")
AWS_REGION_S3    = os.environ.get("AWS_REGION_S3",   "ap-south-2")
S3_BUCKET_MAIN   = os.environ.get("S3_BUCKET_MAIN",  "news-689186650531-ap-south-2-an")
S3_BUCKET_M      = os.environ.get("S3_BUCKET_M",     "localaitv1-689186650531-ap-south-2-an")
ECS_CLUSTER      = os.environ.get("ECS_CLUSTER",     "laitv-stg-app-cluster")
SQS_BUILD_URL    = os.environ.get("SQS_BUILD_URL",   "")
API_BASE         = os.environ.get("LOCALAITV_API_URL","https://localaitv.com")
YOUTUBE_API_KEY  = os.environ.get("YOUTUBE_API_KEY", "")
YT_CHANNEL_ID    = os.environ.get("YT_CHANNEL_ID",   "UClB3scGwKSfe3CmLYYFkDoQ")
NETLIFY_TOKEN    = os.environ.get("NETLIFY_AUTH_TOKEN", "")
NETLIFY_SITE_ID  = os.environ.get("NETLIFY_SITE_ID", "c7fdfd85-219d-4666-8444-7791fe7ee790")
GITHUB_TOKEN     = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO      = os.environ.get("GITHUB_REPO",  "nagarjunak-pixel/localaitv-app")
AI_SERVICES_INSTANCE = os.environ.get("AI_SERVICES_INSTANCE", "i-0f41742259048b4de")

CHANNELS = {
    "LocalAiTV":   {"location_id": 161, "folder": "Khammam"},
    "Kurnool":     {"location_id": 305, "folder": "Kurnool"},
    "Karimnagar":  {"location_id": 75,  "folder": "Karimnagar"},
    "Kakinada":    {"location_id": 209, "folder": "Kakinada"},
    "Nalore":      {"location_id": 285, "folder": "Nalore"},
    "Tirupati":    {"location_id": 335, "folder": "Tirupati"},
    "Guntur":      {"location_id": 344, "folder": "Guntur"},
    "Warangal":    {"location_id": 154, "folder": "Warangal"},
    "Nalgonda":    {"location_id": 141, "folder": "Nalgonda"},
}

logging.basicConfig(level=logging.WARNING)

# ── Helpers ──────────────────────────────────────────────────────────────────

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in environment")
    # 25s connect + 30s statement timeout — needed for Mac→GCP CloudSQL cross-cloud latency
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=25,
        options="-c statement_timeout=30000",
    )


def run_query(sql: str, params=None, max_rows=200) -> list[dict]:
    """Execute a read-only SQL query and return rows as dicts."""
    conn = db_connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchmany(max_rows)
            return [dict(r) for r in rows]
    finally:
        conn.close()


def run_mutation(sql: str, params=None) -> int:
    """Execute an INSERT/UPDATE/DELETE and return rowcount."""
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = cur.rowcount
        conn.commit()
        return rowcount
    finally:
        conn.close()


def aws_client(service: str, region: str = None):
    region = region or AWS_REGION_COMP
    return boto3.client(service, region_name=region)


def fmt_json(obj: Any) -> str:
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)
    return json.dumps(obj, indent=2, default=default)


# ── MCP Server ────────────────────────────────────────────────────────────────

app = Server("localaitv-admin")


@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="pipeline_health",
            description=(
                "Full health snapshot: ECS service status (app + streamer + builder), "
                "SQS queue depth, S3 bucket reachability, DB row counts, and recent error count from logs."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="db_query",
            description=(
                "Run a read-only SELECT query on newsai_db. "
                "Tables: news_items, processed_reports, channel_dispatch_cooldown, reporters, users. "
                "Always SELECT limited rows (add LIMIT). Never pass user input directly — use parameterized queries."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "A SELECT statement. Must start with SELECT."},
                    "max_rows": {"type": "integer", "default": 50, "description": "Max rows to return (cap 200)"},
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="get_news_items",
            description="Fetch recent news items from newsai_db. Filter by channel, status, or date range.",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel":    {"type": "string", "description": "Channel name e.g. Kurnool, Warangal (optional)"},
                    "bulletined": {"type": "integer", "description": "0=pending build, 1=built (optional)"},
                    "hours_back": {"type": "integer", "default": 24, "description": "Look back N hours"},
                    "limit":      {"type": "integer", "default": 30},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="get_channel_status",
            description=(
                "For each of the 9 channels, show: pending item count (bulletined=0), "
                "last bulletin timestamp in S3, last dispatch cooldown, and item counts by hour."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {"type": "string", "description": "Specific channel name, or omit for all 9"},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="get_pending_reports",
            description="List reports that are pending processing, stuck, or failed in the pipeline.",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["pending", "failed", "all"],
                        "default": "pending",
                    },
                    "limit": {"type": "integer", "default": 30},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="get_ecs_services",
            description="Get ECS service status for app, streamer, and builder services — running count, desired, last event.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_app_logs",
            description="Fetch recent CloudWatch log lines from the app or streamer ECS service.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service":    {"type": "string", "enum": ["app", "streamer", "builder"], "default": "app"},
                    "minutes":    {"type": "integer", "default": 30, "description": "Look back N minutes"},
                    "filter":     {"type": "string", "description": "Keyword filter (optional), e.g. ERROR or Kurnool"},
                    "max_lines":  {"type": "integer", "default": 50},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="get_sqs_status",
            description="Get SQS build queue depth (main + DLQ). High DLQ count = failed builds.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_s3_bulletins",
            description="List bulletin MP4 files in S3 for a channel. Shows latest N bulletins with size and timestamp.",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {"type": "string", "description": "Channel name e.g. Kurnool"},
                    "limit":   {"type": "integer", "default": 10},
                },
                "required": ["channel"],
            },
        ),
        types.Tool(
            name="get_reporter_stats",
            description="Reporter activity stats — top reporters by report count, recent submissions, approval rates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "hours_back": {"type": "integer", "default": 24},
                    "top_n":      {"type": "integer", "default": 10},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="search_news_items",
            description="Full-text search across news_items headlines and original_text.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query":  {"type": "string", "description": "Search keyword (Telugu or English)"},
                    "limit":  {"type": "integer", "default": 20},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="reset_channel_bulletins",
            description=(
                "⚠️ MUTATION: Reset bulletined=0 for recent items in a channel so they get rebuilt. "
                "Use this when a channel is stuck or content needs regenerating. "
                "Will NOT affect items older than hours_back."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "channel":    {"type": "string", "description": "Channel name e.g. Kurnool"},
                    "hours_back": {"type": "integer", "default": 6, "description": "Reset items from last N hours only"},
                    "confirm":    {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["channel", "confirm"],
            },
        ),
        types.Tool(
            name="scale_ecs_service",
            description=(
                "⚠️ MUTATION: Scale an ECS service desired count up or down. "
                "Use to start/stop app or streamer. Builder scales via capacity provider automatically."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "service": {"type": "string", "enum": ["app", "streamer"]},
                    "desired": {"type": "integer", "description": "0 to stop, 1 to start"},
                    "confirm": {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["service", "desired", "confirm"],
            },
        ),
        # ── Infrastructure tools ───────────────────────────────────────────────
        types.Tool(
            name="get_ec2_status",
            description="Get status of all LocalAI TV EC2 instances (ai-services, localai-backend, Abhishek dev box) across ap-south-1 and ap-south-2.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="check_disk_usage",
            description="Check disk usage on the ai-services EC2 instance via SSM. It was at 98% full — use this to monitor it.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_aws_costs",
            description="Get this month's AWS cost breakdown by service using Cost Explorer. Shows where the money is going.",
            inputSchema={
                "type": "object",
                "properties": {
                    "days_back": {"type": "integer", "default": 30, "description": "How many days to look back"},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="get_cloudtrail_audit",
            description="Get recent AWS CloudTrail events — who did what and when. Useful for tracking changes by Gyan or other team members.",
            inputSchema={
                "type": "object",
                "properties": {
                    "hours_back":  {"type": "integer", "default": 24},
                    "event_name":  {"type": "string", "description": "Filter by event e.g. DeleteDBInstance, UpdateService (optional)"},
                    "username":    {"type": "string", "description": "Filter by IAM user e.g. localaitv-phase0 (optional)"},
                    "limit":       {"type": "integer", "default": 20},
                },
                "required": [],
            },
        ),
        # ── YouTube & Streaming tools ──────────────────────────────────────────
        types.Tool(
            name="get_stream_health",
            description="Check if all 9 YouTube live streams are actually broadcasting. Uses YouTube Data API to verify stream status.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_live_viewer_counts",
            description="Get concurrent live viewer counts for all 9 channels from YouTube Data API.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_channel_analytics",
            description="Get YouTube channel stats — total views, subscribers, video count for the LocalAI TV channel.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        # ── Reporter management tools ──────────────────────────────────────────
        types.Tool(
            name="get_registration_requests",
            description="List new citizen reporter registration requests waiting for review.",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["pending", "approved", "all"], "default": "pending"},
                    "limit":  {"type": "integer", "default": 20},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="approve_reporter",
            description="⚠️ MUTATION: Approve a citizen reporter registration so they can submit stories.",
            inputSchema={
                "type": "object",
                "properties": {
                    "reporter_id": {"type": "integer", "description": "Reporter ID to approve"},
                    "confirm":     {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["reporter_id", "confirm"],
            },
        ),
        types.Tool(
            name="get_reporter_earnings",
            description="Get reporter earnings/payout data — reports submitted, approved, and pending payment.",
            inputSchema={
                "type": "object",
                "properties": {
                    "reporter_id": {"type": "integer", "description": "Specific reporter ID (optional — omit for all)"},
                    "days_back":   {"type": "integer", "default": 30},
                    "limit":       {"type": "integer", "default": 20},
                },
                "required": [],
            },
        ),
        # ── App & Deploy tools ─────────────────────────────────────────────────
        types.Tool(
            name="get_netlify_deploy_status",
            description="Get the latest deploy status for the LocalAI TV marketing site on Netlify.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_github_repo_status",
            description="Get latest commits, open PRs, and CI status for the LocalAI TV GitHub repository.",
            inputSchema={
                "type": "object",
                "properties": {
                    "repo": {"type": "string", "description": "Repo in owner/name format (optional — defaults to pipeline repo gyanrnk/localaitv1)"},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="check_geo_bulletins",
            description="Check the geo bulletin S3 paths (geo/states/andhra_pradesh/... and geo/states/telangana/...) — verifies the new geo bulletin uploads are working after the migration.",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {"type": "string", "description": "Channel name to check geo path (optional — checks all if omitted)"},
                },
                "required": [],
            },
        ),
        # ── Session 2 tools ────────────────────────────────────────────────────
        types.Tool(
            name="approve_report",
            description=(
                "⚠️ MUTATION: Approve a citizen reporter's news item so it gets picked up for bulletin building. "
                "Sets approved=true (or bulletined=0 + claimed_at=NULL) on the news_items row. "
                "Use when a reporter's story is stuck in unapproved state."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "item_id": {"type": "integer", "description": "news_items.id to approve"},
                    "confirm": {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["item_id", "confirm"],
            },
        ),
        types.Tool(
            name="reject_report",
            description=(
                "⚠️ MUTATION: Reject a news item — marks it so it will never be built into a bulletin. "
                "Sets bulletined=2 (rejected sentinel) on the news_items row."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "item_id": {"type": "integer", "description": "news_items.id to reject"},
                    "reason":  {"type": "string", "description": "Optional reason (logged but not stored in DB)"},
                    "confirm": {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["item_id", "confirm"],
            },
        ),
        types.Tool(
            name="trigger_bulletin_build",
            description=(
                "⚠️ MUTATION: Force-trigger a bulletin build for a channel right now by resetting "
                "the dispatch cooldown and setting MIN_FRESH_ITEMS override to 1. "
                "Use when a channel has content but isn't building (e.g. Kurnool stuck at 2 items)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {"type": "string", "description": "Channel name e.g. Kurnool"},
                    "confirm": {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["channel", "confirm"],
            },
        ),
        types.Tool(
            name="purge_dlq",
            description=(
                "⚠️ MUTATION: Purge all messages from the SQS Dead Letter Queue (failed build jobs). "
                "Use when DLQ has stale failed jobs that should be discarded. "
                "Content will rebuild naturally on next planner cycle. IRREVERSIBLE."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "confirm": {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["confirm"],
            },
        ),
        types.Tool(
            name="replay_dlq",
            description=(
                "⚠️ MUTATION: Move messages from the Dead Letter Queue back to the main build queue for retry. "
                "Only use AFTER fixing the underlying failure (e.g. after deploying intro-path fix). "
                "Otherwise jobs will fail again and return to DLQ."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "max_messages": {"type": "integer", "default": 10, "description": "How many DLQ messages to replay (max 50)"},
                    "confirm":      {"type": "boolean", "description": "Must be true to execute"},
                },
                "required": ["confirm"],
            },
        ),
        types.Tool(
            name="get_bulletin_preview_url",
            description=(
                "Generate a presigned S3 URL to watch a bulletin MP4 directly in the browser. "
                "URL expires in 1 hour. Use after get_s3_bulletins to get a key, then pass it here."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "channel":      {"type": "string", "description": "Channel name e.g. Kurnool (optional — used to find latest bulletin automatically)"},
                    "s3_key":       {"type": "string", "description": "Exact S3 key e.g. bulletins/Kurnool/bul_20260624_120000.mp4 (optional)"},
                    "expires_secs": {"type": "integer", "default": 3600, "description": "Link expiry in seconds"},
                },
                "required": [],
            },
        ),
    ]


# ── Tool Handlers ─────────────────────────────────────────────────────────────

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        result = await _dispatch(name, arguments)
    except Exception as e:
        result = f"❌ Error in {name}: {type(e).__name__}: {e}"
    return [types.TextContent(type="text", text=str(result))]


async def _dispatch(name: str, args: dict) -> str:
    loop = asyncio.get_event_loop()

    if name == "pipeline_health":
        return await loop.run_in_executor(None, _pipeline_health)

    elif name == "db_query":
        sql = args["sql"].strip()
        if not sql.upper().startswith("SELECT"):
            return "❌ Only SELECT queries are allowed via db_query."
        rows = await loop.run_in_executor(None, lambda: run_query(sql, max_rows=args.get("max_rows", 50)))
        return fmt_json({"row_count": len(rows), "rows": rows})

    elif name == "get_news_items":
        return await loop.run_in_executor(None, lambda: _get_news_items(args))

    elif name == "get_channel_status":
        return await loop.run_in_executor(None, lambda: _get_channel_status(args.get("channel")))

    elif name == "get_pending_reports":
        return await loop.run_in_executor(None, lambda: _get_pending_reports(args))

    elif name == "get_ecs_services":
        return await loop.run_in_executor(None, _get_ecs_services)

    elif name == "get_app_logs":
        return await loop.run_in_executor(None, lambda: _get_app_logs(args))

    elif name == "get_sqs_status":
        return await loop.run_in_executor(None, _get_sqs_status)

    elif name == "get_s3_bulletins":
        return await loop.run_in_executor(None, lambda: _get_s3_bulletins(args))

    elif name == "get_reporter_stats":
        return await loop.run_in_executor(None, lambda: _get_reporter_stats(args))

    elif name == "search_news_items":
        return await loop.run_in_executor(None, lambda: _search_news_items(args))

    elif name == "reset_channel_bulletins":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _reset_channel_bulletins(args))

    elif name == "scale_ecs_service":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _scale_ecs_service(args))

    elif name == "get_ec2_status":
        return await loop.run_in_executor(None, _get_ec2_status)

    elif name == "check_disk_usage":
        return await loop.run_in_executor(None, _check_disk_usage)

    elif name == "get_aws_costs":
        return await loop.run_in_executor(None, lambda: _get_aws_costs(args))

    elif name == "get_cloudtrail_audit":
        return await loop.run_in_executor(None, lambda: _get_cloudtrail_audit(args))

    elif name == "get_stream_health":
        return await loop.run_in_executor(None, _get_stream_health)

    elif name == "get_live_viewer_counts":
        return await loop.run_in_executor(None, _get_live_viewer_counts)

    elif name == "get_channel_analytics":
        return await loop.run_in_executor(None, _get_channel_analytics)

    elif name == "get_registration_requests":
        return await loop.run_in_executor(None, lambda: _get_registration_requests(args))

    elif name == "approve_reporter":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _approve_reporter(args))

    elif name == "get_reporter_earnings":
        return await loop.run_in_executor(None, lambda: _get_reporter_earnings(args))

    elif name == "get_netlify_deploy_status":
        return await loop.run_in_executor(None, _get_netlify_deploy_status)

    elif name == "get_github_repo_status":
        return await loop.run_in_executor(None, lambda: _get_github_repo_status(args))

    elif name == "check_geo_bulletins":
        return await loop.run_in_executor(None, lambda: _check_geo_bulletins(args))

    elif name == "approve_report":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _approve_report(args))

    elif name == "reject_report":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _reject_report(args))

    elif name == "trigger_bulletin_build":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _trigger_bulletin_build(args))

    elif name == "purge_dlq":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, _purge_dlq)

    elif name == "replay_dlq":
        if not args.get("confirm"):
            return "❌ Set confirm=true to execute this mutation."
        return await loop.run_in_executor(None, lambda: _replay_dlq(args))

    elif name == "get_bulletin_preview_url":
        return await loop.run_in_executor(None, lambda: _get_bulletin_preview_url(args))

    else:
        return f"❌ Unknown tool: {name}"


# ── Tool Implementations ───────────────────────────────────────────────────────

def _pipeline_health() -> str:
    results = {}

    # 1. DB connectivity + row counts
    try:
        rows = run_query("""
            SELECT
              COUNT(*) FILTER (WHERE bulletined = 0 AND timestamp > NOW() - INTERVAL '24h') AS pending_24h,
              COUNT(*) FILTER (WHERE bulletined = 1 AND timestamp > NOW() - INTERVAL '24h') AS built_24h,
              COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1h') AS ingested_1h,
              MAX(timestamp) AS latest_item
            FROM news_items
        """)
        results["database"] = {"status": "✅ connected", **rows[0]}
    except Exception as e:
        results["database"] = {"status": f"❌ {e}"}

    # 2. ECS services
    try:
        results["ecs"] = _get_ecs_services_dict()
    except Exception as e:
        results["ecs"] = {"status": f"❌ {e}"}

    # 3. SQS
    try:
        results["sqs"] = _get_sqs_dict()
    except Exception as e:
        results["sqs"] = {"status": f"❌ {e}"}

    # 4. S3 reachability (quick head_bucket)
    try:
        s3 = aws_client("s3", region=AWS_REGION_S3)
        s3.head_bucket(Bucket=S3_BUCKET_MAIN)
        results["s3_main"] = "✅ reachable"
    except Exception as e:
        results["s3_main"] = f"❌ {e}"

    # 5. Recent error count from CloudWatch
    try:
        logs = aws_client("logs")
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        ago_ms = int((datetime.utcnow() - timedelta(hours=1)).timestamp() * 1000)
        resp = logs.filter_log_events(
            logGroupName="/ecs/laitv-stg/app",
            startTime=ago_ms, endTime=now_ms,
            filterPattern="ERROR",
            limit=10,
        )
        results["errors_last_1h"] = len(resp.get("events", []))
    except Exception as e:
        results["errors_last_1h"] = f"unavailable ({e})"

    return fmt_json(results)


def _get_ecs_services_dict() -> dict:
    ecs = aws_client("ecs")
    service_names = ["laitv-stg-app", "laitv-stg-streamer"]
    resp = ecs.describe_services(cluster=ECS_CLUSTER, services=service_names)
    out = {}
    for svc in resp.get("services", []):
        name = svc["serviceName"]
        latest_event = svc.get("events", [{}])[0].get("message", "")[:120] if svc.get("events") else ""
        out[name] = {
            "desired": svc["desiredCount"],
            "running": svc["runningCount"],
            "pending": svc["pendingCount"],
            "rollout": svc.get("deployments", [{}])[0].get("rolloutState", ""),
            "latest_event": latest_event,
        }
    return out


def _get_ecs_services() -> str:
    return fmt_json(_get_ecs_services_dict())


def _get_sqs_dict() -> dict:
    if not SQS_BUILD_URL:
        return {"status": "SQS_BUILD_URL not configured"}
    sqs = aws_client("sqs")
    attrs = sqs.get_queue_attributes(
        QueueUrl=SQS_BUILD_URL,
        AttributeNames=["ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed"],
    )["Attributes"]
    # DLQ
    dlq_url = SQS_BUILD_URL.replace("laitv-stg-build", "laitv-stg-build-dlq")
    try:
        dlq_attrs = sqs.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        dlq_depth = dlq_attrs.get("ApproximateNumberOfMessages", "?")
    except Exception:
        dlq_depth = "unavailable"
    return {
        "visible": attrs.get("ApproximateNumberOfMessages"),
        "in_flight": attrs.get("ApproximateNumberOfMessagesNotVisible"),
        "dlq_depth": dlq_depth,
    }


def _get_sqs_status() -> str:
    return fmt_json(_get_sqs_dict())


def _get_news_items(args: dict) -> str:
    channel = args.get("channel")
    bulletined = args.get("bulletined")
    hours_back = args.get("hours_back", 24)
    limit = min(args.get("limit", 30), 200)

    wheres = ["timestamp > NOW() - INTERVAL '%s hours'" % int(hours_back)]
    params = []

    if channel and channel in CHANNELS:
        wheres.append("location_id = %s")
        params.append(CHANNELS[channel]["location_id"])

    if bulletined is not None:
        wheres.append("bulletined = %s")
        params.append(int(bulletined))

    sql = f"""
        SELECT id, location_id, headline, bulletined, claimed_at,
               timestamp, media_type
        FROM news_items
        WHERE {' AND '.join(wheres)}
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    rows = run_query(sql, params or None, max_rows=limit)
    return fmt_json({"count": len(rows), "rows": rows})


def _get_channel_status(channel_filter: str | None) -> str:
    channels = {channel_filter: CHANNELS[channel_filter]} if (channel_filter and channel_filter in CHANNELS) else CHANNELS
    results = {}
    for ch_name, ch_info in channels.items():
        loc_id = ch_info["location_id"]
        try:
            stats = run_query("""
                SELECT
                  COUNT(*) FILTER (WHERE bulletined = 0 AND timestamp > NOW() - INTERVAL '24h') AS pending,
                  COUNT(*) FILTER (WHERE bulletined = 1 AND timestamp > NOW() - INTERVAL '24h') AS built,
                  COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1h')  AS last_1h,
                  MAX(timestamp) AS latest_item,
                  MAX(claimed_at) AS last_claimed
                FROM news_items
                WHERE location_id = %s
            """, (loc_id,))
            # cooldown
            cooldown = run_query("""
                SELECT last_dispatch_at FROM channel_dispatch_cooldown
                WHERE location_id = %s
            """, (loc_id,))
            row = stats[0] if stats else {}
            row["last_dispatch"] = cooldown[0]["last_dispatch_at"] if cooldown else None
            results[ch_name] = row
        except Exception as e:
            results[ch_name] = {"error": str(e)}
    return fmt_json(results)


def _get_pending_reports(args: dict) -> str:
    status = args.get("status", "pending")
    limit = min(args.get("limit", 30), 200)

    if status == "pending":
        where = "processed = false OR processed IS NULL"
    elif status == "failed":
        where = "error_message IS NOT NULL"
    else:
        where = "1=1"

    sql = f"""
        SELECT id, report_id, status, error_message, created_at, updated_at
        FROM processed_reports
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    try:
        rows = run_query(sql, max_rows=limit)
    except Exception:
        # table name might differ
        sql2 = f"""
            SELECT id, headline, media_type, bulletined, claimed_at, timestamp as created_at
            FROM news_items
            WHERE bulletined = 0 AND timestamp > NOW() - INTERVAL '48h'
            ORDER BY timestamp DESC LIMIT {limit}
        """
        rows = run_query(sql2, max_rows=limit)
    return fmt_json({"count": len(rows), "rows": rows})


def _get_app_logs(args: dict) -> str:
    service = args.get("service", "app")
    minutes = min(args.get("minutes", 30), 360)
    filt = args.get("filter", "")
    max_lines = min(args.get("max_lines", 50), 200)

    log_group_map = {
        "app":      "/ecs/laitv-stg/app",
        "streamer": "/ecs/laitv-stg/streamer",
        "builder":  "/ecs/laitv-stg/builder",
    }
    log_group = log_group_map.get(service, "/ecs/laitv-stg/app")

    logs = aws_client("logs")
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    ago_ms = int((datetime.utcnow() - timedelta(minutes=minutes)).timestamp() * 1000)

    kwargs = dict(
        logGroupName=log_group,
        startTime=ago_ms,
        endTime=now_ms,
        limit=max_lines,
    )
    if filt:
        kwargs["filterPattern"] = filt

    resp = logs.filter_log_events(**kwargs)
    events = resp.get("events", [])
    lines = [
        f"[{datetime.utcfromtimestamp(e['timestamp']/1000).strftime('%H:%M:%S')}] {e['message'].rstrip()}"
        for e in events
    ]
    return f"Log group: {log_group}\nLast {minutes}min | filter='{filt}' | {len(lines)} lines\n\n" + "\n".join(lines)


def _get_s3_bulletins(args: dict) -> str:
    channel = args["channel"]
    limit = min(args.get("limit", 10), 50)

    if channel not in CHANNELS:
        return f"❌ Unknown channel '{channel}'. Valid: {', '.join(CHANNELS)}"

    folder = CHANNELS[channel]["folder"]
    s3 = aws_client("s3", region=AWS_REGION_S3)

    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET_MAIN,
        Prefix=f"bulletins/{folder}/",
        Delimiter="/",
    )
    # bul_ subfolders
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    # sort descending, take limit
    prefixes = sorted(prefixes, reverse=True)[:limit]

    items = []
    for prefix in prefixes:
        bul_name = prefix.rstrip("/").split("/")[-1]
        # get mp4 inside
        inner = s3.list_objects_v2(Bucket=S3_BUCKET_MAIN, Prefix=prefix)
        for obj in inner.get("Contents", []):
            if obj["Key"].endswith(".mp4"):
                items.append({
                    "bulletin": bul_name,
                    "key": obj["Key"],
                    "size_mb": round(obj["Size"] / 1_000_000, 1),
                    "last_modified": obj["LastModified"],
                })
    # also check flat bulletins
    resp2 = s3.list_objects_v2(Bucket=S3_BUCKET_MAIN, Prefix=f"bulletins/{folder}/bul_")
    for obj in sorted(resp2.get("Contents", []), key=lambda x: x["LastModified"], reverse=True)[:limit]:
        if obj["Key"].endswith(".mp4"):
            items.append({
                "bulletin": obj["Key"].split("/")[-1],
                "key": obj["Key"],
                "size_mb": round(obj["Size"] / 1_000_000, 1),
                "last_modified": obj["LastModified"],
            })

    items = sorted(items, key=lambda x: str(x.get("last_modified", "")), reverse=True)[:limit]
    return fmt_json({"channel": channel, "folder": folder, "count": len(items), "bulletins": items})


def _get_reporter_stats(args: dict) -> str:
    hours_back = args.get("hours_back", 24)
    top_n = min(args.get("top_n", 10), 50)
    try:
        rows = run_query(f"""
            SELECT
              reporter_id,
              COUNT(*) AS report_count,
              COUNT(*) FILTER (WHERE bulletined = 1) AS built_count,
              MAX(timestamp) AS last_report
            FROM news_items
            WHERE timestamp > NOW() - INTERVAL '{int(hours_back)} hours'
              AND reporter_id IS NOT NULL
            GROUP BY reporter_id
            ORDER BY report_count DESC
            LIMIT {top_n}
        """, max_rows=top_n)
        return fmt_json({"top_reporters_last_%dh" % hours_back: rows})
    except Exception as e:
        return f"❌ {e}"


def _search_news_items(args: dict) -> str:
    query = args["query"]
    limit = min(args.get("limit", 20), 100)
    rows = run_query("""
        SELECT id, location_id, headline, bulletined, timestamp, media_type
        FROM news_items
        WHERE (headline ILIKE %s OR original_text ILIKE %s)
          AND timestamp > NOW() - INTERVAL '7 days'
        ORDER BY timestamp DESC
        LIMIT %s
    """, (f"%{query}%", f"%{query}%", limit), max_rows=limit)
    return fmt_json({"query": query, "count": len(rows), "rows": rows})


def _reset_channel_bulletins(args: dict) -> str:
    channel = args["channel"]
    hours_back = args.get("hours_back", 6)

    if channel not in CHANNELS:
        return f"❌ Unknown channel '{channel}'. Valid: {', '.join(CHANNELS)}"

    loc_id = CHANNELS[channel]["location_id"]
    rowcount = run_mutation("""
        UPDATE news_items
        SET bulletined = 0, claimed_at = NULL
        WHERE location_id = %s
          AND timestamp > NOW() - INTERVAL '%s hours'
          AND bulletined = 1
    """ % ("%s", int(hours_back)), (loc_id,))
    return f"✅ Reset {rowcount} items in {channel} (location_id={loc_id}) to bulletined=0. Pipeline will rebuild them."


def _scale_ecs_service(args: dict) -> str:
    service_key = args["service"]
    desired = int(args["desired"])

    service_map = {
        "app":      "laitv-stg-app",
        "streamer": "laitv-stg-streamer",
    }
    svc_name = service_map[service_key]

    ecs = aws_client("ecs")
    ecs.update_service(cluster=ECS_CLUSTER, service=svc_name, desiredCount=desired)
    return f"✅ Scaled {svc_name} → desired={desired}. Allow ~60s for ECS to converge."


# ── Infrastructure Implementations ───────────────────────────────────────────

def _get_ec2_status() -> str:
    results = {}
    for region in [AWS_REGION_COMP, AWS_REGION_S3]:
        try:
            ec2 = aws_client("ec2", region=region)
            resp = ec2.describe_instances(
                Filters=[{"Name": "tag:project", "Values": ["*localaitv*", "*laitv*", "*phase0*"]}]
            )
            instances = []
            for r in resp["Reservations"]:
                for i in r["Instances"]:
                    name = next((t["Value"] for t in i.get("Tags", []) if t["Key"] == "Name"), "unnamed")
                    instances.append({
                        "name":       name,
                        "id":         i["InstanceId"],
                        "type":       i["InstanceType"],
                        "state":      i["State"]["Name"],
                        "public_ip":  i.get("PublicIpAddress", "none"),
                        "launch_time": i.get("LaunchTime"),
                    })
            if not instances:
                # fallback: describe known instance IDs directly
                known = [AI_SERVICES_INSTANCE, "i-02bb825fdf5100199", "i-00d1aa2f09b2ecf61"]
                resp2 = ec2.describe_instances(InstanceIds=[i for i in known if i])
                for r in resp2["Reservations"]:
                    for i in r["Instances"]:
                        name = next((t["Value"] for t in i.get("Tags", []) if t["Key"] == "Name"), i["InstanceId"])
                        instances.append({
                            "name": name, "id": i["InstanceId"],
                            "type": i["InstanceType"], "state": i["State"]["Name"],
                            "public_ip": i.get("PublicIpAddress", "none"),
                            "launch_time": i.get("LaunchTime"),
                        })
            results[region] = instances
        except Exception as e:
            results[region] = {"error": str(e)}
    return fmt_json(results)


def _check_disk_usage() -> str:
    ssm = aws_client("ssm", region=AWS_REGION_S3)
    try:
        resp = ssm.send_command(
            InstanceIds=[AI_SERVICES_INSTANCE],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["df -h / /var /tmp 2>/dev/null || df -h"]},
        )
        cmd_id = resp["Command"]["CommandId"]
        import time; time.sleep(3)
        out = ssm.get_command_invocation(
            CommandId=cmd_id, InstanceId=AI_SERVICES_INSTANCE
        )
        stdout = out.get("StandardOutputContent", "")
        stderr = out.get("StandardErrorContent", "")
        status = out.get("Status", "")
        return f"Instance: {AI_SERVICES_INSTANCE}\nStatus: {status}\n\n{stdout or stderr or 'No output'}"
    except Exception as e:
        return f"❌ SSM error: {e}\nNote: ai-services instance must be SSM-managed/Online."


def _get_aws_costs(args: dict) -> str:
    days_back = min(int(args.get("days_back", 30)), 90)
    ce = aws_client("ce", region="us-east-1")
    end   = datetime.utcnow().date()
    start = (datetime.utcnow() - timedelta(days=days_back)).date()
    try:
        resp = ce.get_cost_and_usage(
            TimePeriod={"Start": str(start), "End": str(end)},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        rows = []
        total = 0.0
        for result in resp["ResultsByTime"]:
            for group in sorted(result["Groups"], key=lambda g: float(g["Metrics"]["UnblendedCost"]["Amount"]), reverse=True):
                svc = group["Keys"][0]
                amt = float(group["Metrics"]["UnblendedCost"]["Amount"])
                if amt > 0.01:
                    rows.append({"service": svc, "cost_usd": round(amt, 2)})
                    total += amt
        rows.append({"service": "TOTAL", "cost_usd": round(total, 2)})
        return fmt_json({"period": f"{start} to {end}", "costs": rows})
    except Exception as e:
        return f"❌ Cost Explorer error: {e}\nNote: Cost Explorer must be enabled in the AWS account."


def _get_cloudtrail_audit(args: dict) -> str:
    hours_back = min(int(args.get("hours_back", 24)), 168)
    limit      = min(int(args.get("limit", 20)), 50)
    event_name = args.get("event_name", "")
    username   = args.get("username", "")
    ct = aws_client("cloudtrail", region=AWS_REGION_COMP)
    kwargs = {
        "StartTime": datetime.utcnow() - timedelta(hours=hours_back),
        "EndTime":   datetime.utcnow(),
        "MaxResults": limit,
    }
    lookup_attrs = []
    if event_name:
        lookup_attrs.append({"AttributeKey": "EventName", "AttributeValue": event_name})
    if username:
        lookup_attrs.append({"AttributeKey": "Username", "AttributeValue": username})
    if lookup_attrs:
        kwargs["LookupAttributes"] = [lookup_attrs[0]]
    try:
        resp = ct.lookup_events(**kwargs)
        events = []
        for e in resp.get("Events", []):
            events.append({
                "time":       e.get("EventTime"),
                "event":      e.get("EventName"),
                "user":       e.get("Username", ""),
                "source_ip":  json.loads(e.get("CloudTrailEvent", "{}")).get("sourceIPAddress", ""),
                "resources":  [r.get("ResourceName", "") for r in e.get("Resources", [])][:2],
            })
        return fmt_json({"period_hours": hours_back, "count": len(events), "events": events})
    except Exception as e:
        return f"❌ CloudTrail error: {e}"


# ── YouTube Implementations ───────────────────────────────────────────────────

def _yt_get(path: str, params: dict) -> dict:
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in .env")
    params["key"] = YOUTUBE_API_KEY
    with httpx.Client(timeout=10) as client:
        r = client.get(f"https://www.googleapis.com/youtube/v3/{path}", params=params)
        r.raise_for_status()
        return r.json()


def _get_stream_health() -> str:
    try:
        data = _yt_get("search", {
            "channelId": YT_CHANNEL_ID,
            "eventType": "live",
            "type": "video",
            "part": "id,snippet",
            "maxResults": 15,
        })
        live_items = data.get("items", [])
        live_titles = [i["snippet"]["title"] for i in live_items]
        channel_names = list(CHANNELS.keys())
        status = {}
        for ch in channel_names:
            matched = any(ch.lower() in t.lower() for t in live_titles)
            status[ch] = "✅ LIVE" if matched else "🔴 NOT FOUND in live streams"
        status["_total_live_streams_found"] = len(live_items)
        status["_live_titles"] = live_titles
        return fmt_json(status)
    except Exception as e:
        return f"❌ YouTube API error: {e}\nSet YOUTUBE_API_KEY in .env to use this tool."


def _get_live_viewer_counts() -> str:
    try:
        data = _yt_get("search", {
            "channelId": YT_CHANNEL_ID,
            "eventType": "live",
            "type": "video",
            "part": "id,snippet",
            "maxResults": 15,
        })
        video_ids = [i["id"]["videoId"] for i in data.get("items", [])]
        if not video_ids:
            return "No live streams found on the channel right now."
        stats = _yt_get("videos", {
            "id": ",".join(video_ids),
            "part": "snippet,liveStreamingDetails",
        })
        results = []
        for v in stats.get("items", []):
            title   = v["snippet"]["title"]
            viewers = v.get("liveStreamingDetails", {}).get("concurrentViewers", "0")
            results.append({"title": title, "live_viewers": int(viewers)})
        results.sort(key=lambda x: x["live_viewers"], reverse=True)
        total = sum(r["live_viewers"] for r in results)
        return fmt_json({"total_viewers": total, "channels": results})
    except Exception as e:
        return f"❌ YouTube API error: {e}\nSet YOUTUBE_API_KEY in .env to use this tool."


def _get_channel_analytics() -> str:
    try:
        data = _yt_get("channels", {
            "id": YT_CHANNEL_ID,
            "part": "snippet,statistics,contentDetails",
        })
        items = data.get("items", [])
        if not items:
            return f"❌ Channel {YT_CHANNEL_ID} not found."
        ch = items[0]
        stats = ch.get("statistics", {})
        return fmt_json({
            "channel_name":    ch["snippet"]["title"],
            "subscribers":     stats.get("subscriberCount", "hidden"),
            "total_views":     stats.get("viewCount", "0"),
            "video_count":     stats.get("videoCount", "0"),
            "published_at":    ch["snippet"].get("publishedAt"),
            "country":         ch["snippet"].get("country", "IN"),
        })
    except Exception as e:
        return f"❌ YouTube API error: {e}\nSet YOUTUBE_API_KEY in .env to use this tool."


# ── Reporter Management Implementations ───────────────────────────────────────

def _get_registration_requests(args: dict) -> str:
    status = args.get("status", "pending")
    limit  = min(int(args.get("limit", 20)), 100)
    where  = {"pending": "approved = false OR approved IS NULL",
               "approved": "approved = true",
               "all": "1=1"}.get(status, "1=1")
    try:
        rows = run_query(f"""
            SELECT id, name, phone, location_id, created_at, approved
            FROM reporters
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT {limit}
        """, max_rows=limit)
        return fmt_json({"status_filter": status, "count": len(rows), "reporters": rows})
    except Exception as e:
        return f"❌ {e} — table may be named differently (users/reporter_profiles)"


def _approve_reporter(args: dict) -> str:
    reporter_id = int(args["reporter_id"])
    rows = run_query("SELECT id, name, phone FROM reporters WHERE id = %s", (reporter_id,))
    if not rows:
        return f"❌ No reporter found with id={reporter_id}"
    r = rows[0]
    run_mutation("UPDATE reporters SET approved = true WHERE id = %s", (reporter_id,))
    return f"✅ Approved reporter {reporter_id} — {r.get('name', '')} ({r.get('phone', '')})"


def _get_reporter_earnings(args: dict) -> str:
    reporter_id = args.get("reporter_id")
    days_back   = int(args.get("days_back", 30))
    limit       = min(int(args.get("limit", 20)), 100)
    params = []
    where_parts = [f"timestamp > NOW() - INTERVAL '{days_back} days'"]
    if reporter_id:
        where_parts.append("reporter_id = %s")
        params.append(int(reporter_id))
    where = " AND ".join(where_parts)
    try:
        rows = run_query(f"""
            SELECT
                reporter_id,
                COUNT(*)                                   AS total_submitted,
                COUNT(*) FILTER (WHERE bulletined = 1)     AS built_into_bulletin,
                COUNT(*) FILTER (WHERE bulletined = 0)     AS pending,
                COUNT(*) FILTER (WHERE bulletined = 2)     AS rejected,
                MAX(timestamp)                             AS last_submission
            FROM news_items
            WHERE {where} AND reporter_id IS NOT NULL
            GROUP BY reporter_id
            ORDER BY total_submitted DESC
            LIMIT {limit}
        """, params or None, max_rows=limit)
        return fmt_json({"days_back": days_back, "count": len(rows), "earnings_data": rows})
    except Exception as e:
        return f"❌ {e}"


# ── App & Deploy Implementations ──────────────────────────────────────────────

def _get_netlify_deploy_status() -> str:
    if not NETLIFY_TOKEN:
        return "❌ NETLIFY_AUTH_TOKEN not set in .env"
    with httpx.Client(timeout=10) as client:
        r = client.get(
            f"https://api.netlify.com/api/v1/sites/{NETLIFY_SITE_ID}/deploys",
            headers={"Authorization": f"Bearer {NETLIFY_TOKEN}"},
            params={"per_page": 5},
        )
        r.raise_for_status()
        deploys = r.json()
    results = []
    for d in deploys[:5]:
        results.append({
            "id":         d.get("id", "")[:8],
            "state":      d.get("state"),
            "created_at": d.get("created_at"),
            "deploy_url": d.get("deploy_url", ""),
            "branch":     d.get("branch", ""),
            "error_msg":  d.get("error_message", ""),
        })
    latest = results[0] if results else {}
    state_icon = {"ready": "✅", "error": "❌", "building": "⏳"}.get(latest.get("state", ""), "❓")
    return fmt_json({
        "site_id":        NETLIFY_SITE_ID,
        "latest_status":  f"{state_icon} {latest.get('state', 'unknown')}",
        "latest_url":     latest.get("deploy_url", ""),
        "recent_deploys": results,
    })


def _get_github_repo_status(args: dict) -> str:
    repo = args.get("repo", "gyanrnk/localaitv1")
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    with httpx.Client(timeout=10, headers=headers) as client:
        commits_r = client.get(f"https://api.github.com/repos/{repo}/commits", params={"per_page": 5})
        prs_r     = client.get(f"https://api.github.com/repos/{repo}/pulls",   params={"state": "open", "per_page": 5})
        runs_r    = client.get(f"https://api.github.com/repos/{repo}/actions/runs", params={"per_page": 3})
    commits = []
    if commits_r.status_code == 200:
        for c in commits_r.json():
            commits.append({
                "sha":     c["sha"][:7],
                "message": c["commit"]["message"].split("\n")[0][:80],
                "author":  c["commit"]["author"]["name"],
                "date":    c["commit"]["author"]["date"],
            })
    prs = []
    if prs_r.status_code == 200:
        for p in prs_r.json():
            prs.append({"number": p["number"], "title": p["title"][:60], "author": p["user"]["login"]})
    runs = []
    if runs_r.status_code == 200:
        for r in runs_r.json().get("workflow_runs", []):
            runs.append({"name": r["name"], "status": r["status"], "conclusion": r.get("conclusion"), "branch": r["head_branch"]})
    return fmt_json({
        "repo":           repo,
        "latest_commits": commits,
        "open_prs":       prs,
        "ci_runs":        runs,
    })


def _check_geo_bulletins(args: dict) -> str:
    channel_filter = args.get("channel")
    channels = {channel_filter: CHANNELS[channel_filter]} if (channel_filter and channel_filter in CHANNELS) else CHANNELS
    s3 = aws_client("s3", region=AWS_REGION_S3)
    state_map = {
        "LocalAiTV": "telangana", "Karimnagar": "telangana", "Warangal": "telangana",
        "Nalgonda":  "telangana", "Khammam":    "telangana",
        "Kurnool":   "andhra_pradesh", "Kakinada": "andhra_pradesh",
        "Nalore":    "andhra_pradesh", "Tirupati": "andhra_pradesh", "Guntur": "andhra_pradesh",
    }
    results = {}
    for ch_name, ch_info in channels.items():
        folder = ch_info["folder"]
        state  = state_map.get(ch_name, "andhra_pradesh")
        geo_prefix = f"geo/states/{state}/districts/{folder.lower()}/news_bulletins/"
        try:
            resp = s3.list_objects_v2(Bucket=S3_BUCKET_MAIN, Prefix=geo_prefix, MaxKeys=5)
            objects = resp.get("Contents", [])
            latest = sorted(objects, key=lambda x: x["LastModified"], reverse=True)
            results[ch_name] = {
                "geo_path":   geo_prefix,
                "count":      len(objects),
                "latest":     latest[0]["Key"].split("/")[-1] if latest else None,
                "latest_ts":  latest[0]["LastModified"] if latest else None,
                "status":     "✅ geo bulletins present" if objects else "🔴 no geo bulletins found",
            }
        except Exception as e:
            results[ch_name] = {"error": str(e)}
    return fmt_json(results)


# ── Session 2 Tool Implementations ───────────────────────────────────────────

def _approve_report(args: dict) -> str:
    item_id = int(args["item_id"])
    # First check the item exists
    rows = run_query("SELECT id, headline, bulletined, location_id FROM news_items WHERE id = %s", (item_id,))
    if not rows:
        return f"❌ No news_item found with id={item_id}"
    item = rows[0]
    if item.get("bulletined") == 1:
        return f"ℹ️ Item {item_id} is already built (bulletined=1). Headline: {item.get('headline', '')[:60]}"
    # Reset to buildable state
    rowcount = run_mutation(
        "UPDATE news_items SET bulletined = 0, claimed_at = NULL WHERE id = %s",
        (item_id,)
    )
    return (
        f"✅ Approved item {item_id} — set bulletined=0, claimed_at=NULL.\n"
        f"Headline: {item.get('headline', '')[:80]}\n"
        f"It will be picked up on the next planner cycle (within 5 min)."
    )


def _reject_report(args: dict) -> str:
    item_id = int(args["item_id"])
    reason = args.get("reason", "no reason given")
    rows = run_query("SELECT id, headline FROM news_items WHERE id = %s", (item_id,))
    if not rows:
        return f"❌ No news_item found with id={item_id}"
    item = rows[0]
    # bulletined=2 = rejected sentinel (never built)
    run_mutation("UPDATE news_items SET bulletined = 2, claimed_at = NULL WHERE id = %s", (item_id,))
    return (
        f"✅ Rejected item {item_id} (bulletined=2 — will never be built).\n"
        f"Headline: {item.get('headline', '')[:80]}\n"
        f"Reason logged: {reason}"
    )


def _trigger_bulletin_build(args: dict) -> str:
    channel = args["channel"]
    if channel not in CHANNELS:
        return f"❌ Unknown channel '{channel}'. Valid: {', '.join(CHANNELS)}"
    loc_id = CHANNELS[channel]["location_id"]

    # 1. Clear dispatch cooldown so planner doesn't wait
    run_mutation(
        "DELETE FROM channel_dispatch_cooldown WHERE location_id = %s",
        (loc_id,)
    )
    # 2. Reset oldest unbuilt items' claimed_at so they're claimable
    run_mutation(
        """
        UPDATE news_items
        SET claimed_at = NULL
        WHERE location_id = %s
          AND bulletined = 0
          AND timestamp > NOW() - INTERVAL '48 hours'
        """,
        (loc_id,)
    )
    # 3. Count how many items are now ready
    ready = run_query(
        "SELECT COUNT(*) AS cnt FROM news_items WHERE location_id = %s AND bulletined = 0 AND timestamp > NOW() - INTERVAL '48h'",
        (loc_id,)
    )
    cnt = ready[0]["cnt"] if ready else "?"
    return (
        f"✅ Triggered build for {channel} (location_id={loc_id}).\n"
        f"  • Dispatch cooldown cleared\n"
        f"  • claimed_at reset on all pending items\n"
        f"  • {cnt} items ready to build\n"
        f"Planner will pick this up within ~5 minutes."
    )


def _purge_dlq() -> str:
    if not SQS_BUILD_URL:
        return "❌ SQS_BUILD_URL not configured"
    dlq_url = SQS_BUILD_URL.replace("laitv-stg-build", "laitv-stg-build-dlq")
    sqs = aws_client("sqs")
    # Get count before purge
    attrs = sqs.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    count_before = attrs.get("ApproximateNumberOfMessages", "?")
    sqs.purge_queue(QueueUrl=dlq_url)
    return (
        f"✅ DLQ purged — {count_before} failed build jobs deleted.\n"
        f"Queue: {dlq_url}\n"
        f"Note: AWS takes up to 60s to reflect the purge in queue depth metrics."
    )


def _replay_dlq(args: dict) -> str:
    if not SQS_BUILD_URL:
        return "❌ SQS_BUILD_URL not configured"
    max_msgs = min(int(args.get("max_messages", 10)), 50)
    dlq_url = SQS_BUILD_URL.replace("laitv-stg-build", "laitv-stg-build-dlq")
    sqs = aws_client("sqs")
    replayed = 0
    errors = []
    while replayed < max_msgs:
        batch = min(10, max_msgs - replayed)
        resp = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=batch,
            WaitTimeSeconds=2,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            try:
                sqs.send_message(QueueUrl=SQS_BUILD_URL, MessageBody=msg["Body"])
                sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=msg["ReceiptHandle"])
                replayed += 1
            except Exception as e:
                errors.append(str(e))
    result = f"✅ Replayed {replayed} messages from DLQ → main build queue."
    if errors:
        result += f"\n⚠️ {len(errors)} errors: {errors[0]}"
    return result


def _get_bulletin_preview_url(args: dict) -> str:
    s3_key = args.get("s3_key", "")
    channel = args.get("channel", "")
    expires = min(int(args.get("expires_secs", 3600)), 86400)

    # If no key given, find latest bulletin for channel
    if not s3_key and channel:
        if channel not in CHANNELS:
            return f"❌ Unknown channel '{channel}'. Valid: {', '.join(CHANNELS)}"
        folder = CHANNELS[channel]["folder"]
        s3 = aws_client("s3", region=AWS_REGION_S3)
        resp = s3.list_objects_v2(
            Bucket=S3_BUCKET_MAIN,
            Prefix=f"bulletins/{folder}/bul_",
        )
        objects = sorted(
            [o for o in resp.get("Contents", []) if o["Key"].endswith(".mp4")],
            key=lambda x: x["LastModified"],
            reverse=True,
        )
        if not objects:
            # Try subfolder pattern
            resp2 = s3.list_objects_v2(Bucket=S3_BUCKET_MAIN, Prefix=f"bulletins/{folder}/")
            all_mp4 = []
            for pfx in resp2.get("CommonPrefixes", []):
                inner = s3.list_objects_v2(Bucket=S3_BUCKET_MAIN, Prefix=pfx["Prefix"])
                all_mp4 += [o for o in inner.get("Contents", []) if o["Key"].endswith(".mp4")]
            objects = sorted(all_mp4, key=lambda x: x["LastModified"], reverse=True)
        if not objects:
            return f"❌ No bulletins found for {channel} in S3."
        s3_key = objects[0]["Key"]

    if not s3_key:
        return "❌ Provide either channel or s3_key."

    s3 = aws_client("s3", region=AWS_REGION_S3)
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET_MAIN, "Key": s3_key},
        ExpiresIn=expires,
    )
    size_info = ""
    try:
        head = s3.head_object(Bucket=S3_BUCKET_MAIN, Key=s3_key)
        size_mb = round(head["ContentLength"] / 1_000_000, 1)
        size_info = f"\nSize: {size_mb} MB"
    except Exception:
        pass

    return (
        f"✅ Presigned URL (expires in {expires//60} min):\n\n"
        f"{url}\n\n"
        f"Key: {s3_key}{size_info}\n\n"
        f"Open this link in any browser to watch the bulletin."
    )


# ── Entry Point ───────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read, write):
        await app.run(read, write, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
