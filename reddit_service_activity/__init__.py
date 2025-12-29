import json
import logging
import math
import random
import re

import redis

from baseplate import Baseplate
from baseplate.lib import config

# Import redis client utilities from new baseplate location
from baseplate.clients.redis import RedisContextFactory, pool_from_config

# Import thrift integration - try new location first, fall back to old
try:
    from baseplate.frameworks.thrift import baseplateify_processor
except ImportError:
    try:
        from baseplate.integration.thrift import BaseplateProcessorEventHandler
        baseplateify_processor = None
    except ImportError:
        BaseplateProcessorEventHandler = None
        baseplateify_processor = None

# Try to import generated Thrift stubs; provide fallbacks if not available
try:
    from .activity_thrift import ActivityService, ttypes
    _HAS_THRIFT = True
except ImportError:
    _HAS_THRIFT = False
    ActivityService = None
    ttypes = None

from .counter import ActivityCounter


logger = logging.getLogger(__name__)
_ID_RE = re.compile("^[A-Za-z0-9_]{,50}$")
_CACHE_TIME = 30  # seconds


# Define ActivityInfo - extend thrift type if available, otherwise standalone
if _HAS_THRIFT and ttypes is not None:
    class ActivityInfo(ttypes.ActivityInfo):
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
else:
    # Standalone ActivityInfo when thrift stubs are not available
    class ActivityInfo:
        def __init__(self, count=0, is_fuzzed=False):
            self.count = count
            self.is_fuzzed = is_fuzzed

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
            if isinstance(value, (tuple, list)):
                return cls(count=value[0], is_fuzzed=value[1])
            deserialized = json.loads(value)
            return cls(
                count=deserialized["count"],
                is_fuzzed=deserialized["is_fuzzed"],
            )


# Provide InvalidContextIDException - use from activity_client for consistency with tests
from .activity_client import InvalidContextIDException

# Use thrift exception if available, otherwise use our module-level exception
if _HAS_THRIFT and ActivityService is not None:
    _InvalidContextIDException = getattr(ActivityService, 'InvalidContextIDException', InvalidContextIDException)
else:
    _InvalidContextIDException = InvalidContextIDException


# Define Handler - inherit from thrift interface if available
if _HAS_THRIFT and ActivityService is not None:
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
                raise _InvalidContextIDException()

            activity = {}

            # read cached activity
            cache_keys = [context_id + "/cached" for context_id in context_ids]
            cached_info = context.redis.mget(cache_keys)
            for context_id, cached_value in zip(context_ids, cached_info):
                if cached_value is None:
                    continue
                # Handle both bytes and string values from redis
                if hasattr(cached_value, 'decode'):
                    cached_value = cached_value.decode()
                activity[context_id] = ActivityInfo.from_json(cached_value)

            # count any ones that were not cached
            missing_ids = [id_ for id_ in context_ids if id_ not in activity]
            if not missing_ids:
                return activity

            # First pipeline: count activity
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

            # Second pipeline: cache results
            if to_cache:
                with context.redis.pipeline("cache", transaction=False) as pipe:
                    for context_id, info in list(to_cache.items()):
                        pipe.setex(context_id + "/cached", _CACHE_TIME, info.to_json())
                    pipe.execute()

            return activity
else:
    # Fallback Handler when thrift stubs are not available
    class Handler:
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
                raise _InvalidContextIDException()

            activity = {}

            # read cached activity
            cache_keys = [context_id + "/cached" for context_id in context_ids]
            cached_info = context.redis.mget(cache_keys)
            for context_id, cached_value in zip(context_ids, cached_info):
                if cached_value is None:
                    continue
                # Handle both bytes and string values from redis
                if hasattr(cached_value, 'decode'):
                    cached_value = cached_value.decode()
                activity[context_id] = ActivityInfo.from_json(cached_value)

            # count any ones that were not cached
            missing_ids = [id_ for id_ in context_ids if id_ not in activity]
            if not missing_ids:
                return activity

            # First pipeline: count activity
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

            # Second pipeline: cache results
            if to_cache:
                with context.redis.pipeline("cache", transaction=False) as pipe:
                    for context_id, info in to_cache.items():
                        pipe.setex(context_id + "/cached", _CACHE_TIME, info.to_json())
                    pipe.execute()

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

    redis_pool = pool_from_config(app_config, prefix="redis.")

    baseplate = Baseplate(app_config)
    baseplate.configure_observers()
    baseplate.add_to_context("redis", RedisContextFactory(redis_pool))

    counter = ActivityCounter(cfg.activity.window.total_seconds())
    handler = Handler(counter=counter)

    if _HAS_THRIFT and ActivityService is not None:
        processor = ActivityService.ContextProcessor(handler)
        if baseplateify_processor is not None:
            processor = baseplateify_processor(processor, logger, baseplate)
        elif BaseplateProcessorEventHandler is not None:
            event_handler = BaseplateProcessorEventHandler(logger, baseplate)
            processor.setEventHandler(event_handler)
        return processor

    return handler
