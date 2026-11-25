"""Redis quote cache. Derived from PostgreSQL and never authoritative: losing it loses nothing.

Writes are conditional on event time (atomically, in Redis), so a late or replayed event,
or a rebuild racing live traffic, can never replace a newer quote with an older one.
"""

from __future__ import annotations

import json
from collections.abc import Iterable

from redis import Redis

QUOTES_KEY = "mp:quotes"
TICKS_CHANNEL = "mp:ticks"

# KEYS[1] = quotes hash, KEYS[2] = channel; ARGV = symbol, event_ts_ms, quote json, publish (1/0)
_SET_IF_NEWER = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current then
  local ok, q = pcall(cjson.decode, current)
  if ok and type(q) == 'table' and tonumber(q['event_ts_ms']) and tonumber(q['event_ts_ms']) > tonumber(ARGV[2]) then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
if ARGV[4] == '1' then redis.call('PUBLISH', KEYS[2], ARGV[3]) end
return 1
"""


class QuoteCache:
    def __init__(self, redis: Redis, quotes_key: str = QUOTES_KEY, channel: str = TICKS_CHANNEL) -> None:
        self._redis = redis
        self._key = quotes_key
        self._channel = channel
        self._set_if_newer = redis.register_script(_SET_IF_NEWER)

    def write(self, quotes: Iterable[dict], bars: Iterable[dict] = (), publish: bool = True) -> list[bool]:
        """Store quotes that are newer than the cached ones (and publish those); publish bars.

        Returns, per quote, whether it was stored.
        """
        pipe = self._redis.pipeline(transaction=False)
        quotes = list(quotes)
        for q in quotes:
            self._set_if_newer(
                keys=[self._key, self._channel],
                args=[q["symbol"], q["event_ts_ms"], json.dumps(q, separators=(",", ":")), "1" if publish else "0"],
                client=pipe,
            )
        if publish:
            for b in bars:
                pipe.publish(self._channel, json.dumps(b, separators=(",", ":")))
        results = pipe.execute()
        return [bool(r) for r in results[: len(quotes)]]

    def missing(self, symbols: Iterable[str]) -> list[str]:
        present = {k.decode() if isinstance(k, bytes) else k for k in self._redis.hkeys(self._key)}
        return [s for s in symbols if s not in present]
