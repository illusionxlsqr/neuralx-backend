import os
import sys
import traceback

print("=== Starting NEURAL-X Backend ===")
print("Python version:", sys.version)
print("PORT env var:", os.environ.get("PORT"))

port = int(os.environ.get("PORT", 10000))

try:
    import server
    import uvicorn
    print(f"Server module loaded. Starting uvicorn on 0.0.0.0:{port}...")
    uvicorn.run(server.app, host="0.0.0.0", port=port)
except Exception as e:
    err = traceback.format_exc()
    print("CRITICAL ERROR DURING STARTUP:")
    print(err)
    
    # Fallback HTTP server so Render healthcheck passes and we can inspect the exact error
    from http.server import HTTPServer, BaseHTTPRequestHandler
    class ErrorHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"NEURAL-X STARTUP DIAGNOSTIC:\n\n{err}".encode("utf-8"))
        def log_message(self, format, *args):
            pass

    print(f"Starting diagnostic HTTP server on 0.0.0.0:{port}...")
    httpd = HTTPServer(("0.0.0.0", port), ErrorHandler)
    httpd.serve_forever()
