#!/usr/bin/env python3
"""Loopback stand-in for the two Central Portal publisher endpoints the release build calls.

Usage: central_portal_stub.py <port file> <bundle output path>

Listens on 127.0.0.1 on a free port, writes the port number to <port file>, and serves:
  POST /api/v1/publisher/upload  saves the multipart "bundle" part to <bundle output path>, 201
  POST /api/v1/publisher/status  reports the deployment as PUBLISHED
Both require an Authorization header with the UserToken or Bearer scheme, as Central does (the
plugin sends UserToken). Anything else is a 404. It lets central-dry-run.sh run
the real deploy (bundle, upload, wait until published) without any network access, so the dry run
checks exactly the bundle a release would upload. It is a test fixture, not a model of Central's
validation.
"""

import email.parser
import email.policy
import http.server
import json
import sys
import urllib.parse

DEPLOYMENT_ID = "00000000-0000-0000-0000-00000000d2y0"


class Handler(http.server.BaseHTTPRequestHandler):
    bundle_path = None
    deployment_name = None

    def log_message(self, fmt, *args):
        sys.stderr.write("central stub: " + (fmt % args) + "\n")

    def _body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            data = bytearray()
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                if size == 0:
                    self.rfile.readline()
                    return bytes(data)
                data += self.rfile.read(size)
                self.rfile.readline()
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def _reply(self, status, body, content_type="text/plain"):
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        url = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(url.query)
        body = self._body()
        if not self.headers.get("Authorization", "").startswith(("UserToken ", "Bearer ")):
            self._reply(401, "missing UserToken or Bearer authorization")
            return
        if url.path == "/api/v1/publisher/upload":
            message = email.parser.BytesParser(policy=email.policy.HTTP).parsebytes(
                b"Content-Type: " + self.headers.get("Content-Type", "").encode("latin-1")
                + b"\r\n\r\n" + body)
            parts = [p for p in message.iter_parts()
                     if p.get_param("name", header="content-disposition") == "bundle"]
            if len(parts) != 1:
                self._reply(400, "expected one multipart part named bundle")
                return
            with open(Handler.bundle_path, "wb") as f:
                f.write(parts[0].get_payload(decode=True))
            Handler.deployment_name = (query.get("name") or ["Deployment"])[0]
            sys.stderr.write("central stub: received bundle for %s (publishingType=%s)\n"
                             % (Handler.deployment_name, (query.get("publishingType") or ["?"])[0]))
            self._reply(201, DEPLOYMENT_ID)
        elif url.path == "/api/v1/publisher/status":
            self._reply(200, json.dumps({
                "deploymentId": (query.get("id") or [DEPLOYMENT_ID])[0],
                "deploymentName": Handler.deployment_name or "Deployment",
                "deploymentState": "PUBLISHED",
                "purls": [],
            }), "application/json")
        else:
            self._reply(404, "not part of the stub")


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    Handler.bundle_path = argv[2]
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    with open(argv[1], "w") as f:
        f.write(str(server.server_address[1]))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
