"""Hold one real committed cleanup reply outside the product process."""
import http.server
import json
import threading

class CleanupReplyProxy:
    def __init__(self, client, port, barrier=None):
        if barrier is None:
            self.committed=threading.Event()
            self.release=threading.Event()
            self.lock=threading.Lock()
            self.observed=None
        proxy=barrier or self
        self.release=proxy.release
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                if self.headers.get('Authorization')!='Bearer '+client.config['Token']:
                    self.send_error(401);return
                self.connection.settimeout(5)
                length=int(self.headers.get('Content-Length','0'))
                if length<0 or length>4*1024*1024:
                    self.send_error(413);return
                body=json.loads(self.rfile.read(length))
                result=None
                for node in client.config['Nodes']:
                    if node['Group']!='a':continue
                    try:
                        result=client.call(node,'/internal',body,timeout=2)
                        break
                    except OSError:continue
                if result is None:
                    self.send_error(503);return
                hold=False
                if body.get('Op')=='cleanup' and not result.get('error'):
                    with proxy.lock:
                        if proxy.observed is None:
                            proxy.observed={'command':body,'response':result}
                            proxy.committed.set()
                        hold=not proxy.release.is_set()
                if hold and not proxy.release.wait(timeout=20):
                    self.send_error(504);return
                payload=json.dumps(result).encode()
                try:
                    self.send_response(200)
                    self.send_header('Content-Length',str(len(payload)))
                    self.end_headers();self.wfile.write(payload)
                except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):pass
        self.server=http.server.ThreadingHTTPServer(('127.0.0.1',port),Handler)
        self.server.daemon_threads=True
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()

    def close(self):
        self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=3)
