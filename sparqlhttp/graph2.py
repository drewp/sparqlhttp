import urllib, md5, logging, restkit
from rdflib import RDFS
from rdflib.exceptions import UniquenessError
from twisted.internet import defer
from sparqlhttp.sparqljson import parseJsonResults
from sparqlhttp.remotegraph import interpolateSparql, _checkQuerySyntax, _addOptionalVars, makeDeferredFunc, graphFromTriples
log = logging.getLogger()

class _Graph2(object):
    def __init__(self, protocol, target, cache=None, initNs=None):
        """
        :Parameters:
            protocol
                a supported protocol, one of: 'sesame', 'rdflib-berkeleydb'
            target
                base URI of remote server.

                For sesame, this is the http server's URL up through
                .../repositories/{repo} without the trailing slash.

                For rdflib-berkeleydb, this is a plain path to the
                directory of the berkeleydb store.
                
            cache
                Optional cache scheme object instance that can handle
                get/set calls.

            initNs
                Dict of namespace prefix to URIRefs that will be used
                on all queries (so you don't have to pass PREFIX lines
                each time)

        You may even ask for an Async version of an rdflib-berkeleydb
        graph, which simply wraps all the results in deferreds. This
        may be useful for testing.
        """
        self.prologue = ""
        if initNs:
            self.prologue = "".join("PREFIX %s: <%s>\n" % (pre,full)
                                    for (pre,full) in initNs.items())
        self.resultFormat = 'json' # need this? can't we negotiate?
        self._setRoot(target)
        self.target = target

    def _getQuery(self, query, initBindings, headers=None, _postProcess=None):
        """send this query to the server, return deferred to the raw
        server result. This is where the prologue (@PREFIX lines) is added."""
        _checkQuerySyntax(query)
        try:
            get = ('?query=' +
                   urllib.quote(self.prologue +
                                interpolateSparql(query, initBindings), safe=''))
        except Exception:
            log.error("original query=%r, initBindings=%r" %
                      (query, initBindings))
            raise

        # for the stats system to group together all the queries that
        # vary only in their initBindings, I send a (reasonably)
        # unique value for the uninterpolated query too. If I could
        # send the query&bindings separately, this wouldn't be
        # necessary.
        xBindings = (" ".join(initBindings.keys())).encode('utf8')

        sendHeaders = {'x-uninterpolated-query-checksum' :
                       md5.new(query).hexdigest(),
                       'x-bindings' : xBindings,
                       }
        if self.resultFormat == 'json':
            sendHeaders['Accept'] = 'application/sparql-results+json'
        else:
            sendHeaders['Accept'] = 'application/sparql-results+xml'
            
        if headers:
            sendHeaders.update(headers)

        def post(ret):
            ret = _addOptionalVars(parseJsonResults(ret), query)
            if _postProcess is not None:
                ret = _postProcess(ret)
            return ret
        return self._request(
            "GET", path='',
            queryParams=[
                ('query', self.prologue +
                 interpolateSparql(query, initBindings))],
            headers=sendHeaders,
            postProcess=post,
            )
        
    def queryd(self, query, initBindings={}, _postProcess=None):
        return self._getQuery(query, initBindings, _postProcess=_postProcess)
 
    def countQuery(self, query, initBindings={}, _postProcess=None):
        def post(rows):
            ret = len(rows)
            if _postProcess:
                ret = _postProcess(ret)
            return ret
        return self.queryd(query, initBindings, _postProcess=post)

    def add(self, triples, context): 
        g = graphFromTriples(triples)
        postData = g.serialize(format='nt')
        
        return self._request(method="POST", path='/statements',
                             headers={'Content-type' : 'text/plain'},
                             queryParams=[('context', context.n3())],
                             payload=postData)

    def contains(self, stmt):
        bindings = {}
        for termName, value in zip(['s', 'p', 'o'], stmt):
            if value is not None:
                bindings[termName] = value
            
        def check(n):
            return n > 0

        return self.countQuery("SELECT * WHERE { ?s ?p ?o. }",
                          initBindings=bindings, _postProcess=check)

    def label(self, subj, default=''):
        return self.value(subj, RDFS.label, default=default, any=True)

    def value(self, subj, pred, default=None, any=False):
        def justObject(rows):
            if len(rows) == 0:
                return default
            if len(rows) > 1 and not any:
                raise UniquenessError(values=[row['o'] for row in rows])
            return rows[0]['o']
        return self.queryd("SELECT DISTINCT ?o WHERE { ?s ?p ?o }",
                           {'s' : subj, 'p' : pred},
                           _postProcess=justObject)
    
    def subgraphLength(self, context):
        raise NotImplementedError
    def subgraphClear(self, context):
        raise NotImplementedError
    def dumpAllStatements(self):
        raise NotImplementedError
    
    def remove(self, triples, context=None):
        # more efficient and atomic would be to use the xml txn
        # format. But this was faster to write.

        ctxArgs = {}
        if context:
            ctxArgs['context'] = context.n3()
        if not isinstance(self, SyncGraph):
            # just switch to txn, and then it'll be one request again
            # and this won't be a probelm
            raise NotImplementedError

        for stmt in triples:
            self._request(method="DELETE", path="/statements",
                          queryParams=[
                              ('subj', stmt[0].n3()),
                              ('pred', stmt[1].n3()),
                              ('obj', stmt[2].n3()),
                              ] +
                          ([('context', context.n3())] if context else []),
                          )
        
    def save(self, context):                  # for certain remote graphs
        log.warn("not saving %s" % context)

    # only for backwards compatibility with the old version. These
    # ought to all raise a warning
    remoteValue = makeDeferredFunc(value)
    remoteLabel = makeDeferredFunc(label)
    remoteQueryd = makeDeferredFunc(queryd)
    remoteSave = save
    remoteContains = makeDeferredFunc(contains)
    def remoteAdd(self, *triples, **ctx):
        return defer.succeed(self.add(triples, **ctx))
    def remoteRemove(self, *triples, **ctx):
        return defer.succeed(self.remove(triples, **ctx))

class SyncGraph(_Graph2):
    """
    Synchonous remote graph access. You must use SyncGraph or
    AsyncGraph, and this is the right one if you don't use twisted.
    """
    def _setRoot(self, rootUrl):
        self._resource = restkit.Resource(rootUrl)
    def _request(self, method, path, queryParams,
                headers=None, payload=None, postProcess=None):
        """
        path is added to rootUrl
        """
        response = self._resource.do_request(
            method=method,
            url=self.target+path+'?'+urllib.urlencode(queryParams),
            headers=headers,
            payload=payload,
            )
        ret = response.body
        if not response.status.startswith('2'):
            raise ValueError("status %s: %s" % (response.status, ret))
        if postProcess is not None:
            ret = postProcess(ret)
        return ret

class AsyncGraph(_Graph2):
    """
    Asynchronous remote graph access using deferreds. You must use
    SyncGraph or AsyncGraph, and this is the right one if you use
    twisted and want deferred results from the network operations.

    In this class, all public methods return deferred.
    """
    def _setRoot(self, rootUrl):
        self._root = rootUrl
    def _request(self, method, path, queryParams,
                headers=None, payload=None, postProcess=None):
        assert payload is None, 'notimplemented'
        assert method is 'GET', 'notimplemented'
        d = getPage(self._root + path + '?' + urllib.urlencode(queryParams),
                       headers=headers)
        if postProcess is not None:
            d.addCallback(postProcess)
        return d
       # ...
        def notError(err):
            # workaround for twisted treating 204 as an error
            if err.value.status.startswith('2'):
                return err.value.response
            err.raiseException()
        d.addErrback(notError)
