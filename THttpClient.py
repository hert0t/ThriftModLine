from io import BytesIO
import ssl
import ctypes
import warnings
import msgspec
import pybase64
from six.moves import urllib
from six.moves import http_client

from thrift.transport.TTransport import TTransportBase

lib = ctypes.cdll.LoadLibrary("./http2.so")
lib.Call.argtypes = [ctypes.c_char_p]
lib.Call.restype = ctypes.c_char_p
lib.Prepare()

class THttpClient(TTransportBase):

    def __init__(self, uri_or_host, port=None, path=None, cafile=None, cert_file=None, key_file=None, ssl_context=None):
        if port is not None:
            warnings.warn(
                "Please use the THttpClient('http{s}://host:port/path') constructor",
                DeprecationWarning,
                stacklevel=2)
            self.host = uri_or_host
            self.port = port
            assert path
            self.path = path
            self.scheme = 'http'
        else:
            parsed = urllib.parse.urlparse(uri_or_host)
            self.scheme = parsed.scheme
            assert self.scheme in ('http', 'https')
            if self.scheme == 'http':
                self.port = parsed.port or http_client.HTTP_PORT
            elif self.scheme == 'https':
                self.port = parsed.port or http_client.HTTPS_PORT
                self.certfile = cert_file
                self.keyfile = key_file
                self.context = ssl.create_default_context(cafile=cafile) if (cafile and not ssl_context) else ssl_context
            self.host = parsed.hostname
            self.path = parsed.path
            if parsed.query:
                self.path += '?%s' % parsed.query

        self.__wbuf = BytesIO()
        self.__http = None
        self.__http_response = None
        self.__timeout = 120
        self.__custom_headers = None

    def setTimeout(self, ms):
        if ms is None:
            self.__timeout = None
        else:
            self.__timeout = ms / 1000.0

    def setCustomHeaders(self, headers):
        self.__custom_headers = headers

    def read(self, sz):
        return self.__rbuf.read(sz)

    def write(self, buf):
        self.__wbuf.write(buf)

    def flush(self):
        data = self.__wbuf.getvalue()
        self.__wbuf = BytesIO()

        headers = {'Content-Type': 'application/x-thrift', 'Content-Length': str(len(data))}
        if self.__custom_headers:headers.update(self.__custom_headers)
        request = {"url": self.scheme+'://'+self.host+self.path,"body": data,"method": "POST","headers": []}
        for k, v in headers.items():request["headers"].append([k,v])
        result = pybase64.standard_b64decode(lib.Call(msgspec.json.encode(request)))
        self.__rbuf = BytesIO(result)
