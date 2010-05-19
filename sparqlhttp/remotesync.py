"""
synchronous version of RemoteGraph, using restkit
"""

from remotegraph import _RemoteGraph
from twisted.internet import defer
import cgi, restkit
assert restkit.__version__ > '1.', "this module uses HttpResponse.body, which showed up around restkit version 1"

def sync(result):
    res = []
    err = []
    result.addCallback(res.append)
    result.addErrback(err.append)
    if err:
        err[0].raiseException()
    if not res:
        raise ValueError("remote callback wasn't fired")
    return res[0]

def makeSync(func):
    def f2(*a, **kw):
        d = func(*a, **kw)
        return sync(d)
    return f2
    

class _RemoteGraphSync(_RemoteGraph):
    def __init__(self, serverUrl, initNs=None, sendSourceLine=False,
                 resultFormat='json'):
        _RemoteGraph.__init__(self, serverUrl, initNs, sendSourceLine,
                              resultFormat)
        self.serverUrl = serverUrl
        self.resource = restkit.Resource(serverUrl, headers={
            "Accept" : "application/sparql-results+xml",
            })
        
    def _serverGet(self, request, headers=None):
        """deferred to the result of GET {serverUrl}{request}"""
        args = cgi.parse_qs(request.lstrip('?'))
        args['headers'] = self._withSourceLineHeaders(headers)
        return defer.succeed(self.resource.get(**args).body)
            
    def remoteSave(self, context):
        return defer.succeed(self.resource.get('save', context=context))

    def remoteRemove(self, *triples, **context):
        context = context.get('context', None)

        # more efficient and atomic would be to use the xml txn
        # format. But this was faster to write.

        ctxArgs = {}
        if context:
            ctxArgs['context'] = context.n3()
        for stmt in triples:
            self.resource.delete(path='statements',
                                 subj=stmt[0].n3(),
                                 pred=stmt[1].n3(),
                                 obj=stmt[2].n3(), **ctxArgs)
        return defer.succeed('ok')
        

    def _deferredPost(self, url, postData, method='POST'):
        path, args = self._splitArgs(url)
        ret = self.resource.request(method, path, payload=postData, 
                                    headers={'Content-Type' : 'text/plain'},
                                    **args)
        return defer.succeed(ret.body)

    def _splitArgs(self, url):
        """
        {self.serverUrl}{request}?{queryargs}
        returns request, queryArgsAsDict

        this is just to undo the encoding that restkit proceeds to do,
        so it would be nicer if we could send our encoded url all the
        way through (or get the data before it's ever encoded)
        """
        assert url.startswith(self.serverUrl), "url %r" % url
        reqPart = url[len(self.serverUrl):]
        if '?' in reqPart:
            top, args = reqPart.split('?', 1)
            return top, cgi.parse_qs(args)
        else:
            return reqPart, {}

class RemoteGraphSync(object):
    """same as RemoteGraph, but nothing returns deferred; they all
    block and return their result
    """
    def __init__(self, serverUrl, initNs=None, sendSourceLine=False,
                 resultFormat='json'):
        self.rgs = _RemoteGraphSync(serverUrl, initNs, sendSourceLine,
                                    resultFormat)
        
    def callSync(self, method, *args, **kw):
        d = getattr(self.rgs, method)(*args, **kw)
        res = []
        d.addCallback(res.append)
        if not res:
            raise ValueError("remote callback wasn't ready")
        return res[0]

    def makeSync(method):
        def m(self, *args, **kw):
            return self.callSync(method, *args, **kw)
        return m

    remoteQueryd     = makeSync('remoteQueryd')
    remoteCountQuery = makeSync('remoteCountQuery')
    remoteAdd        = makeSync('remoteAdd')
    remoteSave       = makeSync('remoteSave')
    remoteContains   = makeSync('remoteContains')
    remoteLabel      = makeSync('remoteLabel')
    remoteValue      = makeSync('remoteValue')
    remoteRemove     = makeSync('remoteRemove')

