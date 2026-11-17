"""
Base class for the Vercel Python functions in api/: JSON responses, CORS and
the OPTIONS preflight, written once instead of in every api/*.py file.

    class handler(JSONHandler):
        def do_GET(self):
            self.send_json(payload, cache="public, max-age=60, s-maxage=60")

Vercel still finds `handler` in each file; it is a BaseHTTPRequestHandler.
"""
import json
from http.server import BaseHTTPRequestHandler

from lib.mcx_config import make_cors_headers, cache_header


class JSONHandler(BaseHTTPRequestHandler):
    allow_methods = "GET, OPTIONS"
    # Cache-Control for a 200 response when a call gives none; None sends no
    # header (the CDN then caches nothing).
    default_cache = None

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        """Reflect the request's Origin when it is allowed (make_cors_headers)."""
        for k, v in make_cors_headers(self.headers.get("Origin", "")).items():
            self.send_header(k, v)
        self.send_header("Access-Control-Allow-Methods", self.allow_methods)
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, data, status=200, cache=None):
        """Write `data` as JSON. `cache` is the Cache-Control for a good 200
        response; any error status, or a {success: false} body, is sent
        no-store so the CDN never holds a failure (cache_header)."""
        body = json.dumps(data, default=str).encode("utf-8")
        cache = cache if cache is not None else self.default_cache
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cache is not None:
            self.send_header("Cache-Control", cache_header(data, cache) if status == 200 else "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, err, status=500):
        self.send_json({"success": False, "error": str(err)[:200]}, status)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()
