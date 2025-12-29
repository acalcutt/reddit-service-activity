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
from baseplate.integration.pyramid import BaseplateConfigurator
from baseplate.lib.thrift_pool import thrift_pool_from_config
from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPServiceUnavailable, HTTPNoContent

from reddit_service_activity.activity_client import Client as ActivityServiceClient
from reddit_service_activity import activity_client as ActivityService


logger = logging.getLogger(__name__)


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

    # Register the thrift client proxy in the request context. Pass the
    # generated-client class (or our local placeholder `ActivityServiceClient`).
    baseplate.add_to_context("activity", ThriftContextFactory(pool, ActivityServiceClient))

    configurator = Configurator(settings=app_config)

    baseplate_configurator = BaseplateConfigurator(baseplate)
    configurator.include(baseplate_configurator.includeme)

    controller = ActivityGateway()
    configurator.add_route("health", "/health", request_method="GET")
    configurator.add_view(controller.is_healthy, route_name="health", renderer="json")

    configurator.add_route("pixel", "/{context_id:[A-Za-z0-9_]{,40}}.png",
                           request_method="GET")
    configurator.add_view(controller.pixel, route_name="pixel")

    return configurator.make_wsgi_app()
