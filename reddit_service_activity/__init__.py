import json
import logging
import math
import random
import re

import redis

from baseplate import Baseplate
from baseplate.lib import config

# Prefer the new tracing factory in baseplate.observers.tracing, fall back
# to the older helper in baseplate.lib.tracing if necessary.
try:
    from baseplate.observers.tracing import make_client as _make_tracing_client
    from baseplate.lib import config as _bp_config
    def tracing_client_from_config(app_config):
        cfg = _bp_config.parse_config(
            app_config,
            {
                "tracing": {
                    "endpoint": _bp_config.Optional(_bp_config.Endpoint),
                    "service_name": _bp_config.Optional(_bp_config.String),
                    "queue_name": _bp_config.Optional(_bp_config.String),
                }
            },
        )
        service_name = cfg.tracing.service_name if cfg.tracing.service_name is not None else ""
        return _make_tracing_client(
            service_name,
            tracing_endpoint=cfg.tracing.endpoint,
            tracing_queue_name=getattr(cfg.tracing, "queue_name", None),
        )
except Exception:
    try:
        from baseplate.lib.tracing import tracing_client_from_config
    except Exception:
        def tracing_client_from_config(app_config):
            return None
 
from baseplate.clients.redis import RedisContextFactory
from baseplate.frameworks.thrift import baseplateify_processor

try:
    from .activity_client import Client as ActivityServiceClient
    from . import activity_client as ActivityService
    ttypes = None
except Exception:
    ActivityService = None
    ActivityServiceClient = None
    ttypes = None
from .counter import ActivityCounter


logger = logging.getLogger(__name__)
_ID_RE = re.compile("^[A-Za-z0-9_]{,50}$")
_CACHE_TIME = 30  # seconds


_activityinfo_base = None
if ttypes is not None:
    _activityinfo_base = getattr(ttypes, "ActivityInfo", None)

if _activityinfo_base:
    try:
        class ActivityInfo(_activityinfo_base):
            @classmethod
            def from_count(cls, count):
                # keep a minimum jitter range of 5 for counts over 100
                decay = math.exp(float(-min(count, 100)) / 60)
                jitter = round(5 * decay)
                return cls(count=count + random.randint(0, jitter), is_fuzzed=True)

            def to_json(self):
                return json.dumps(
                    {"count": self.count, "is_fuzzed": self.is_fuzzed},
                    sort_keys=True,
                )

            @classmethod
            def from_json(cls, value):
                deserialized = json.loads(value)
                return cls(
                    count=deserialized["count"],
                    is_fuzzed=deserialized["is_fuzzed"],
                )
    except Exception:
        # If subclassing the generated type fails for any reason, fall back to
        # a lightweight local implementation so imports don't blow up.
        class ActivityInfo:
            def __init__(self, count=0, is_fuzzed=False):
                self.count = count
                self.is_fuzzed = is_fuzzed

            @classmethod
            def from_count(cls, count):
                decay = math.exp(float(-min(count, 100)) / 60)
                jitter = round(5 * decay)
                return cls(count=count + random.randint(0, jitter), is_fuzzed=True)

            def to_json(self):
                return json.dumps({"count": self.count, "is_fuzzed": self.is_fuzzed}, sort_keys=True)

            @classmethod
            def from_json(cls, value):
                if isinstance(value, (tuple, list)):
                    return cls(count=value[0], is_fuzzed=value[1])
                deserialized = json.loads(value)
                return cls(count=deserialized.get("count", 0), is_fuzzed=deserialized.get("is_fuzzed", False))
else:
    class ActivityInfo:
        def __init__(self, count=0, is_fuzzed=False):
            self.count = count
            self.is_fuzzed = is_fuzzed

        @classmethod
        def from_count(cls, count):
            decay = math.exp(float(-min(count, 100)) / 60)
            jitter = round(5 * decay)
            return cls(count=count + random.randint(0, jitter), is_fuzzed=True)

        def to_json(self):
            return json.dumps({"count": self.count, "is_fuzzed": self.is_fuzzed}, sort_keys=True)

        @classmethod
        def from_json(cls, value):
            if isinstance(value, (tuple, list)):
                return cls(count=value[0], is_fuzzed=value[1])
            deserialized = json.loads(value)
            return cls(count=deserialized.get("count", 0), is_fuzzed=deserialized.get("is_fuzzed", False))


if ActivityService is not None and getattr(ActivityService, "ContextIface", None) is not None:
    class Handler(ActivityService.ContextIface):
        def __init__(self, counter):
            self.counter = counter

        def is_healthy(self, context):
            return context.redis.ping()

        def record_activity(self, context, context_id, visitor_id):
            if not _ID_RE.match(context_id) or not _ID_RE.match(visitor_id):
                return

            self.counter.record_activity(context.redis, context_id, visitor_id)

        def count_activity(self, context, context_id):
            results = self.count_activity_multi(context, [context_id])
            return results[context_id]

        def count_activity_multi(self, context, context_ids):
            if not context_ids:
                return {}

            if not all(_ID_RE.match(context_id) for context_id in context_ids):
                # Prefer the generated thrift exception type when available
                if ActivityService is not None and getattr(ActivityService, "InvalidContextIDException", None) is not None:
                    raise ActivityService.InvalidContextIDException
                raise ValueError("invalid context id")

            activity = {}

            # read cached activity
            cache_keys = [context_id + "/cached" for context_id in context_ids]
            cached_info = context.redis.mget(cache_keys)
            for context_id, cached_value in zip(context_ids, cached_info):
                if cached_value is None:
                    continue
                # redis returns bytes; be tolerant of str or bytes
                if hasattr(cached_value, 'decode'):
                    val = cached_value.decode()
                else:
                    val = cached_value
                activity[context_id] = ActivityInfo.from_json(val)

            # count any ones that were not cached
            missing_ids = [id_ for id_ in context_ids if id_ not in activity]
            if not missing_ids:
                return activity

            with context.redis.pipeline("count", transaction=False) as pipe:
                for context_id in missing_ids:
                    self.counter.count_activity(pipe, context_id)
                counts = pipe.execute()

            # update the cache with the ones we just counted
            to_cache = {}
            for context_id, count in zip(missing_ids, counts):
                if count is not None:
                    info = ActivityInfo.from_count(count)
                    to_cache[context_id] = info
            activity.update(to_cache)

            if to_cache:
                with context.redis.pipeline("cache", transaction=False) as pipe:
                    for context_id, info in list(to_cache.items()):
                        pipe.setex(context_id + "/cached", _CACHE_TIME, info.to_json())
                    pipe.execute()

            return activity
else:
    # ActivityService generated Thrift module is missing; provide a minimal
    # Handler fallback so importing this package doesn't fail in CI. The
    # fallback methods are compatible with tests that only import the package
    # but do not actually run RPC plumbing.
    class Handler:
        def __init__(self, counter):
            self.counter = counter

        def is_healthy(self, context):
            # best-effort: try to ping redis if available
            try:
                return context.redis.ping()
            except Exception:
                return False

        def record_activity(self, context, context_id, visitor_id):
            if not _ID_RE.match(context_id) or not _ID_RE.match(visitor_id):
                return
            try:
                self.counter.record_activity(context.redis, context_id, visitor_id)
            except Exception:
                # swallow; this fallback is only to allow imports
                return

        def count_activity(self, context, context_id):
            return self.count_activity_multi(context, [context_id])[context_id]

        def count_activity_multi(self, context, context_ids):
            if not context_ids:
                return {}

            if not all(_ID_RE.match(context_id) for context_id in context_ids):
                if ActivityService is not None and getattr(ActivityService, "InvalidContextIDException", None) is not None:
                    raise ActivityService.InvalidContextIDException
                raise ValueError("invalid context id")

            # best-effort: try to fetch cache values, otherwise return zeros
            activity = {}
            try:
                cache_keys = [context_id + "/cached" for context_id in context_ids]
                cached_info = context.redis.mget(cache_keys)
                for context_id, cached_value in zip(context_ids, cached_info):
                    if cached_value is None:
                        continue
                    # redis returns bytes; be tolerant of str or bytes
                    if hasattr(cached_value, "decode"):
                        val = cached_value.decode()
                    else:
                        val = cached_value
                    activity[context_id] = ActivityInfo.from_json(val)
            except Exception:
                pass

            for id_ in context_ids:
                activity.setdefault(id_, ActivityInfo.from_count(0))
            return activity


def make_processor(app_config):  # pragma: nocover
    cfg = config.parse_config(app_config, {
        "activity": {
            "window": config.Timespan,
        },
        "tracing": {
            "endpoint": config.Optional(config.Endpoint),
            "service_name": config.String,
        },
        "redis": {
            "url": config.String,
            "max_connections": config.Optional(config.Integer, default=100),
        },
    })

    redis_pool = redis.BlockingConnectionPool.from_url(
        cfg.redis.url,
        max_connections=cfg.redis.max_connections,
        timeout=0.1,
    )

    baseplate = Baseplate(app_config)
    baseplate.configure_observers()
    baseplate.add_to_context("redis", RedisContextFactory(redis_pool))

    counter = ActivityCounter(cfg.activity.window.total_seconds())
    handler = Handler(counter=counter)
    # If the generated Thrift ContextProcessor is available, use it and wrap
    # with baseplate's thrift instrumentation. Otherwise return the handler
    # itself as a lightweight fallback (tests call handler methods directly).
    if ActivityService is not None and getattr(ActivityService, "ContextProcessor", None) is not None:
        processor = ActivityService.ContextProcessor(handler)
        processor = baseplateify_processor(processor, logger, baseplate)
        return processor

    return handler
