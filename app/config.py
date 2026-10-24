import os
from datetime import date

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./de_agent.db")
# Render / Heroku style URLs use postgres://; SQLAlchemy needs postgresql+psycopg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5-5")

# The demo data estate is fictional and frozen at a fixed date so every figure
# on the sandbox is reproducible. Override with AS_OF_DATE=YYYY-MM-DD.
AS_OF_DATE = date.fromisoformat(os.environ.get("AS_OF_DATE", "2026-10-20"))

# Comma-separated origins allowed to call the API from a browser.
CORS_ORIGINS = [o.strip() for o in os.environ.get(
    "CORS_ORIGINS", "https://agilebykillette.com,https://www.agilebykillette.com"
).split(",") if o.strip()]


def _int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, default)).split("#", 1)[0].strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    v = None if v is None else v.split("#", 1)[0].strip()
    return default if not v else v.lower() in ("1", "true", "yes", "on")


# --- Code sandbox ----------------------------------------------------------------
SANDBOX_TIMEOUT_SECONDS = _int("SANDBOX_TIMEOUT_SECONDS", 5)    # hard stop for one test run
SANDBOX_PREVIEW_ROWS = _int("SANDBOX_PREVIEW_ROWS", 25)         # rows returned for preview
SANDBOX_MAX_OUTPUT_ROWS = _int("SANDBOX_MAX_OUTPUT_ROWS", 5000)  # rows kept for a deployed dataset

# --- AI governance: limiter (all configurable via .env) ------------------------
RATE_LIMIT_PER_MINUTE = _int("RATE_LIMIT_PER_MINUTE", 5)        # AI calls per visitor per minute
DAILY_CAP_PER_VISITOR = _int("DAILY_CAP_PER_VISITOR", 25)       # AI calls per visitor per UTC day
DAILY_CAP_GLOBAL = _int("DAILY_CAP_GLOBAL", 200)                # AI calls, all visitors, per UTC day
DAILY_TOKEN_BUDGET = _int("DAILY_TOKEN_BUDGET", 400_000)        # input+output tokens, all visitors, per UTC day
MAX_TOKENS = _int("MAX_TOKENS", 1500)                           # max_tokens per agent model call
MAX_AGENT_TURNS = _int("MAX_AGENT_TURNS", 8)                    # tool-use round trips per question
MAX_REQUEST_BYTES = _int("MAX_REQUEST_BYTES", 32_768)           # any request body
MAX_QUESTION_CHARS = _int("MAX_QUESTION_CHARS", 1000)           # agent question length
# 0 = use the socket peer address. Behind one proxy (Render) use 1.
TRUSTED_PROXY_HOPS = _int("TRUSTED_PROXY_HOPS", 0)
VISITOR_SALT = os.environ.get("VISITOR_SALT", "de-agent-demo")

# --- AI governance: kill switch -----------------------------------------------
# AI_ENABLED=false hard-disables every model call from the environment.
AI_ENABLED = _bool("AI_ENABLED", True)
# If set, POST /api/governance/kill-switch with header X-Admin-Token can flip
# the switch at runtime. If unset, the runtime switch is locked (env only).
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
# If set, every review and deployment decision must carry header X-Reviewer-Token.
REVIEWER_TOKEN = os.environ.get("REVIEWER_TOKEN", "")
