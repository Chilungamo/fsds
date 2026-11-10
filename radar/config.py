"""Runtime configuration read from environment variables."""

from __future__ import annotations

import os

USER_AGENT_ENV = "RADAR_SEC_USER_AGENT"


class ConfigError(RuntimeError):
    pass


def sec_user_agent() -> str:
    """Return the User-Agent sent to SEC EDGAR.

    The SEC fair-access policy requires a User-Agent naming the application and a
    contact address, e.g. ``RestatementRadar/0.1 you@example.com``.
    """
    ua = os.environ.get(USER_AGENT_ENV, "").strip()
    if not ua:
        raise ConfigError(
            f"Set {USER_AGENT_ENV} to an application name plus contact email, e.g. "
            f'export {USER_AGENT_ENV}="RestatementRadar/0.1 you@example.com"'
        )
    return ua


def sec_rate_limit() -> float:
    """Requests per second across this process (SEC allows at most 10)."""
    rate = float(os.environ.get("RADAR_SEC_RATE", "5"))
    return min(max(rate, 0.1), 9.0)
