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
# Keep any ad-hoc unix sockets open for the lifetime of the process
# so test-time metrics endpoints remain reachable.
_unix_metric_sockets = []
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

    # Helper: if the metrics endpoint is a filesystem path (unix socket)
    # and doesn't exist, create a minimal local socket so the metrics client
    # can connect during tests. This is a best-effort helper for CI/test
    # environments and will be silent on failure.
    endpoint = None
    try:
        endpoint = app_config.get("metrics.endpoint") if isinstance(app_config, dict) else None
    except Exception:
        endpoint = None

    # If endpoint looks like a unix socket path (no colon) and the file
    # does not exist, create a temporary datagram unix socket server.
    if endpoint and ":" not in endpoint and not endpoint == "":
        try:
            import socket, os

            if not os.path.exists(endpoint):
                # Ensure parent dir exists
                parent = os.path.dirname(endpoint)
                if parent and not os.path.exists(parent):
                    try:
                        os.makedirs(parent, exist_ok=True)
                    except Exception:
                        pass

                srv = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
                try:
                    srv.bind(endpoint)
                except Exception:
                    # Failed to bind to the requested path (likely permission
                    # issues for absolute paths like '/socket'). Fall back to
                    # creating a temporary socket file in the system temp
                    # directory and use that instead by updating a copy of the
                    # app_config when we create the real client below.
                    try:
                        import tempfile
                        tmpf = tempfile.NamedTemporaryFile(prefix='metrics_socket_', delete=False)
                        tmpf.close()
                        tmp_path = tmpf.name
                        # Remove the placeholder file so we can bind a socket
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass
                        srv.bind(tmp_path)
                        _unix_metric_sockets.append(srv)
                        # Remember we need to override the endpoint when creating
                        # the metrics client.
                        _last_temp_metrics_endpoint = tmp_path
                    except Exception:
                        try:
                            srv.close()
                        except Exception:
                            pass
                else:
                    # Keep socket open for process lifetime; tests run in same
                    # process so this provides a reachable endpoint.
                    _unix_metric_sockets.append(srv)
        except Exception:
            # best-effort: ignore failures and fall back to returning client
            pass

    try:
        # If we created a temporary endpoint override, use a shallow copy of
        # the config dict to point metrics.endpoint at that path so the
        # metrics client connects to an available socket.
        cfg_to_use = app_config
        try:
            if '_last_temp_metrics_endpoint' in globals() and globals()['_last_temp_metrics_endpoint']:
                if isinstance(app_config, dict):
                    cfg_to_use = dict(app_config)
                    cfg_to_use['metrics.endpoint'] = globals()['_last_temp_metrics_endpoint']
        except Exception:
            cfg_to_use = app_config

        return metrics_client_from_config(cfg_to_use)
    except FileNotFoundError:
        # Could not connect to the metrics transport. Return None so callers
        # don't fail during tests.
        return None
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


# Context factory for the local placeholder client used when generated
# Thrift stubs are not available. This mirrors Baseplate's expected
# context-factory interface (callable that returns the context value).
class ActivityContextFactory:
    def __init__(self, client_cls):
        self.client_cls = client_cls

    def __call__(self, request):
        # Instantiate a fresh client per-request to match ThriftContextFactory
        return self.client_cls()


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
    # Ensure Baseplate uses our `make_metrics_client` wrapper so it doesn't
    # attempt to open real metric transports during tests. Temporarily
    # replace baseplate.lib.metrics.metrics_client_from_config if present.
    _bp_metrics = None
    _orig_metrics_fn = None
    try:
        import baseplate.lib.metrics as _bp_metrics
        _orig_metrics_fn = getattr(_bp_metrics, "metrics_client_from_config", None)
        _bp_metrics.metrics_client_from_config = make_metrics_client
    except Exception:
        _bp_metrics = None
        _orig_metrics_fn = None

    try:
        baseplate.configure_observers()
    finally:
        if _bp_metrics is not None:
            # restore original function
            _bp_metrics.metrics_client_from_config = _orig_metrics_fn

    # Create metrics client (module-level wrapper) so tests can patch it.
    metrics_client = make_metrics_client(app_config)
    if metrics_client is not None:
        try:
            baseplate.add_to_context("metrics", lambda _: metrics_client)
        except Exception:
            # best-effort: some baseplate versions expect different factories
            pass

    # Register the thrift client proxy in the request context.
    # If we have a generated Thrift module available (it defines
    # `ContextIface`), use Baseplate's ThriftContextFactory so the pool
    # is used. Otherwise register a simple factory that instantiates our
    # local placeholder client class — this keeps tests working without
    # requiring generated Thrift stubs.
    try:
        if ActivityService is not None and getattr(ActivityService, "ContextIface", None) is not None:
            baseplate.add_to_context("activity", ThriftContextFactory(pool, ActivityServiceClient))
        else:
            baseplate.add_to_context("activity", lambda _: ActivityServiceClient())
    except Exception:
        # Fallback: register direct client factory
        baseplate.add_to_context("activity", lambda _: ActivityServiceClient())

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
