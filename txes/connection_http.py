import codecs
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO
import urllib

import anyjson

from treq

from twisted.internet import defer, reactor, protocol
from twisted.web import client
from twisted.web import iweb
from twisted.web import http
from zope import interface

from txes import exceptions, interfaces, utils


DEFAULT_SERVER = "127.0.0.1:9200"


class HTTPConnection(object):
    interface.implements(interfaces.IConnection)

    def addServer(self, server):
        if server not in self.servers:
            self.servers.append(server)

    def connect(self, servers=None, timeout=None, retryTime=10,
                *args, **kwargs):
        if not servers:
            servers = [DEFAULT_SERVER]
        elif isinstance(servers, (str, unicode)):
            servers = [servers]
        self.servers = utils.ServerList(servers, retryTime=retryTime)
        self.agents = {}

    def close(self):
        pass

    def execute(self, method, path, body=None, params=None):
        server = self.servers.get()
        if not path.startswith('/'):
            path = '/' + path
        url = server + path

        if params:
            url = url + '?' + urllib.urlencode(params)

        if not isinstance(body, basestring):
            body = anyjson.serialize(body)

        if not url.startswith("http://"):
            url = "http://" + url

        def request_done(request):
            def _raise_error(body):
                if request.code != 200:
                    exceptions.raiseExceptions(request.code, body)
                return body
            return treq.json_content(request).addCallback(_raise_error)

        d = treq.request(method, str(url), data=body, persistent=False, timeout=60)
        d.addCallback(request_done)
        return d
