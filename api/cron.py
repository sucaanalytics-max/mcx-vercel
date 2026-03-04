"""
/api/cron — Unified cron dispatcher (Vercel Hobby plan: 12 function limit)

Routes to the correct cron handler based on ?job= query parameter:
  /api/cron?job=valuation   → cron_valuation
  /api/cron?job=models      → cron_models
  /api/cron?job=commodity_signals → cron_commodity_signals
"""
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs

# Import the actual handler classes from underscore-prefixed modules
try:
    from api._cron_valuation import handler as valuation_handler
    from api._cron_models import handler as models_handler
    from api._cron_commodity_signals import handler as signals_handler
except ImportError:
    from _cron_valuation import handler as valuation_handler
    from _cron_models import handler as models_handler
    from _cron_commodity_signals import handler as signals_handler

# Map job names to handler classes
_HANDLERS = {
    "valuation": valuation_handler,
    "models": models_handler,
    "commodity_signals": signals_handler,
}


class handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        job = qs.get("job", [None])[0]

        if not job or job not in _HANDLERS:
            body = json.dumps({
                "error": f"Unknown job: {job}",
                "valid_jobs": list(_HANDLERS.keys()),
            }).encode()
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
            return

        # Delegate to the real handler's do_GET
        # We need to instantiate and call the sub-handler's do_GET
        # But since Vercel passes (request, client_address, server),
        # the simplest approach is to just call the sub-handler class directly
        sub = _HANDLERS[job]
        # Replace self's class to delegate
        self.__class__ = sub
        self.do_GET()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()
