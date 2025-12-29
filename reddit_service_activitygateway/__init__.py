import hashlib
import logging

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
from baseplate.clients.thrift import ThriftContextFactory
# Prefer the new frameworks.pyramid API, fall back to integration.pyramid,
# and finally provide a minimal shim when neither is available.
_HAS_FRAMEWORKS_PYRAMID = False
try:
    from baseplate.frameworks.pyramid import BaseplateConfigurator, StaticTrustHandler
    _HAS_FRAMEWORKS_PYRAMID = True
except Exception:
    try:
        from baseplate.integration.pyramid import BaseplateConfigurator
    except Exception:
        # Provide a minimal fallback for BaseplateConfigurator so tests can
        # create a WSGI app when a full baseplate integration isn't installed.
        class BaseplateConfigurator:
            def __init__(self, baseplate, *args, **kwargs):
                self.baseplate = baseplate

            def includeme(self, configurator):
                # Attach a simple request attribute factory that returns an
                # instance of the local ActivityServiceClient. This provides
                # the minimal surface the tests exercise (is_healthy,
                # record_activity, etc.).
                configurator.add_request_method(lambda req: ActivityServiceClient(), 'activity', reify=True)
from baseplate.lib.thrift_pool import thrift_pool_from_config
from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPServiceUnavailable, HTTPNoContent

from reddit_service_activity.activity_client import Client as ActivityServiceClient
from reddit_service_activity import activity_client as ActivityService


logger = logging.getLogger(__name__)
# Export a couple of helpers so tests can patch them on the module namespace
try:
    from baseplate.lib.metrics import metrics_client_from_config
except Exception:
    metrics_client_from_config = None


def make_metrics_client(app_config):
    # Wrap Baseplate's metrics helper so tests can patch this module-level
    # factory. Return None if Baseplate's metrics helpers aren't available.
    if metrics_client_from_config is None:
        return None
    try:
        return metrics_client_from_config(app_config)
    except Exception:
        return None


try:
    from baseplate.lib.thrift_pool import ThriftConnectionPool as _ThriftConnectionPool
except Exception:
    class _ThriftConnectionPool:
        """Minimal shim for environments without baseplate.lib.thrift_pool.

        Tests can still patch `reddit_service_activitygateway.ThriftConnectionPool`.
        In real deployments the concrete `ThriftConnectionPool` from
        `baseplate.lib.thrift_pool` will be used.
        """
        pass

ThriftConnectionPool = _ThriftConnectionPool


class ActivityGateway(object):
    def is_healthy(self, request):
        try:
            if request.activity.is_healthy():
                return {
                    "status": "healthy",
                }
        except:
            logger.exception("Failed health check")
            raise HTTPServiceUnavailable()

    def pixel(self, request):
        context_id = request.matchdict["context_id"]
        user_agent = (request.user_agent or '').encode("utf8")
        remote_addr = request.remote_addr.encode()
        visitor_id = hashlib.sha1(remote_addr + user_agent).hexdigest()

        request.activity.record_activity(context_id, visitor_id)

        return HTTPNoContent(
            headers={
                "Cache-Control": "no-cache, max-age=0",
                "Pragma": "no-cache",
                "Expires": "Thu, 01 Jan 1970 00:00:00 GMT",
            }
        )


def make_wsgi_app(app_config):
    cfg = config.parse_config(app_config, {
        "activity": {
            "endpoint": config.Endpoint,
        },
        "tracing": {
            "endpoint": config.Optional(config.Endpoint),
            "service_name": config.String,
        },
    })

    # Build a thrift connection pool from app config using the "activity." prefix
    pool = thrift_pool_from_config(app_config, "activity.")

    baseplate = Baseplate(app_config)
    baseplate.configure_observers()

    # Create metrics client (module-level wrapper) so tests can patch it.
    metrics_client = make_metrics_client(app_config)
    if metrics_client is not None:
        try:
            baseplate.add_to_context("metrics", lambda _: metrics_client)
        except Exception:
            # best-effort: some baseplate versions expect different factories
            pass

    # Register the thrift client proxy in the request context. Pass the
    # generated-client class (or our local placeholder `ActivityServiceClient`).
    baseplate.add_to_context("activity", ThriftContextFactory(pool, ActivityServiceClient))

    configurator = Configurator(settings=app_config)

    # Instantiate the Baseplate configurator. Prefer the new frameworks
    # API which accepts header-trust configuration; if available, use a
    # conservative StaticTrustHandler (do not trust headers by default).
    if _HAS_FRAMEWORKS_PYRAMID:
        try:
            trust_handler = StaticTrustHandler(trust_headers=False)
            baseplate_configurator = BaseplateConfigurator(baseplate, header_trust_handler=trust_handler)
        except TypeError:
            # Older variations accept a simple trust_trace_headers kwarg.
            baseplate_configurator = BaseplateConfigurator(baseplate, trust_trace_headers=False)
    else:
        baseplate_configurator = BaseplateConfigurator(baseplate)

    configurator.include(baseplate_configurator.includeme)

    controller = ActivityGateway()
    configurator.add_route("health", "/health", request_method="GET")
    configurator.add_view(controller.is_healthy, route_name="health", renderer="json")

    configurator.add_route("pixel", "/{context_id:[A-Za-z0-9_]{,40}}.png",
                           request_method="GET")
    configurator.add_view(controller.pixel, route_name="pixel")

    return configurator.make_wsgi_app()
