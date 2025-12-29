class InvalidContextIDException(Exception):
    pass


class ActivityInfo:
    def __init__(self, count=0, is_fuzzed=False):
        self.count = count
        self.is_fuzzed = is_fuzzed

    def to_json(self):
        return (self.count, self.is_fuzzed)

    @classmethod
    def from_json(cls, value):
        if isinstance(value, (tuple, list)):
            return cls(count=value[0], is_fuzzed=value[1])
        import json
        d = json.loads(value)
        return cls(count=d.get("count", 0), is_fuzzed=d.get("is_fuzzed", False))


class Client:
    """Minimal, non-Thrift client placeholder used for tests and local runs.

    This implements the small surface the repo expects: `is_healthy`,
    `record_activity`, and `count_activity`/`count_activity_multi`.
    """
    def is_healthy(self):
        return True

    def record_activity(self, context_id, visitor_id):
        # no-op placeholder
        return None

    def count_activity(self, context_id):
        return ActivityInfo(count=0, is_fuzzed=False)

    def count_activity_multi(self, context_ids):
        return {cid: ActivityInfo(count=0, is_fuzzed=False) for cid in context_ids}
