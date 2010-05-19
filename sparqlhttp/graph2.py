import urllib, md5

class _Graph2(object):
    def __init__(self, protocol, target, cache=None):
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

        You may even ask for an Async version of an rdflib-berkeleydb
        graph, which simply wraps all the results in deferreds. This
        may be useful for testing.
        """
       
    def _getQuery(self, query, initBindings, headers=None):
        """send this query to the server, return deferred to the raw
        server result. This is where the prologue (@PREFIX lines) is added."""
        self._checkQuerySyntax(query)
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

        return self._request("GET", self.rootUrl, path='',
                            queryParams=[
                                ('query', self.prologue +
                                 interpolateSparql(query, initBindings))],
                            headers=sendHeaders,
                            postProcess=lambda ret: self._addOptionalVars(parseJsonResults(ret)),
                            )
        
    def queryd(self, query, initBindings={}):
        d = self._getQuery(query, initBindings)
        if self.resultFormat == 'json':
            d.addCallback(parseJsonResults)
        else:
            d.addCallback(parseSparqlResults)
        d.addCallback(self._addOptionalVars, query)
        return d
 
    def countQuery(self, query, initBindings={}):
        raise NotImplementedError
    def add(self, triples, ctx):              # changed to a list, not *args
        raise NotImplementedError
    def contains(self, stmt):
        raise NotImplementedError
    def label(self, subj, default=''):
        raise NotImplementedError
    def value(self, subj, pred, default=None):
        raise NotImplementedError
    def subgraphLength(self, ctx):
        raise NotImplementedError
    def subgraphClear(self, ctx):
        raise NotImplementedError
    def dumpAllStatements(self):
        raise NotImplementedError
    def remove(self, triples, ctx):           # changed to a list, not *args
        raise NotImplementedError
    def save(self, context):                  # for certain remote graphs
        raise NotImplementedError


class SyncGraph(Graph2):
    """
    Synchonous remote graph access. You must use SyncGraph or
    AsyncGraph, and this is the right one if you don't use twisted.
    """
    def _setRoot(self, rootUrl):
        self._resource = restkit.Resource(rootUrl)
    def _request(self, method, path, queryParams,
                headers=None, payload=None, postProcess=None):
        ret = self._resource.request(method)
        if postProcess is not None:
            ret = postProcess(ret)
        return ret

class AsyncGraph(Graph2):
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
