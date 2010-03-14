import urllib, warnings, inspect, os, md5, re
from twisted.internet import defer
from twisted.web.client import getPage
from rdflib import Variable, RDFS, Literal
from rdflib.exceptions import UniquenessError
from rdflib.Graph import Graph
from sparqlhttp.sparqlxml import parseSparqlResults, parseCountTree
from sparqlhttp.dictquery import sparqlSelection
from elementtree import ElementTree

# every http request to the server would print "Stopping factory" and
# then the entire, urlquoted sparql request. That's a mess of
# logs. This is the easiest way I could find to turn them off,
# although it might suppress some good stuff too.
from twisted.web.client import HTTPClientFactory
HTTPClientFactory.noisy = False

# this renaming and factory function is stupid and it interferes with subclassing
class _RemoteGraph(object):
    """this is an async version of Graph2. See below for this async
    API, but as a local version (useful for testing things).

    All queries pass through remoteQueryd or remoteCountQueryd, so
    those are the ones to override when you're adding a cache, for
    example.
    """
    def __init__(self, serverUrl, initNs=None, sendSourceLine=False):
        """
        turn on sendSourceLine and the client will put an
        x-source-line header in every request. The server report shows
        those lines in the query profile screen, which means it'll
        tell you exactly where your slow queries are in the
        source. The tradeoff is that getting the source line (per
        request) can be slow. Maybe milliseconds per req, I forget.
        """
        if not serverUrl.endswith('/'):
            warnings.warn("serverUrl should end with '/', like 'http://localhost:8000/'")
        self.sendSourceLine = sendSourceLine
        self.serverUrl = serverUrl
        self.prologue = ""
        if initNs:
            self.prologue = "".join("PREFIX %s: <%s>\n" % (pre,full)
                                    for (pre,full) in initNs.items())


    # when searching for the code that made calls to RemoteGraph,
    # ignore any filenames in this list. (When subclassing, add your
    # own code's name to the list)
    graphAccessFilenames = ['remotegraph.py']

    def _callerFrame(self):
        """walk up the stack to find the call in the site code
        (as opposed to the calls in this db code, or nevow).

        this takes 5 to 15ms, so it might be nice to turn off for
        production sites
        """
        frames = inspect.getouterframes(inspect.currentframe())
        for frame, filename, line, funcName, code, _ in frames:
            base = os.path.basename(filename)
            # (ought to use the module's real path instead of just
            # guessing with the basenames)
            if base in self.graphAccessFilenames: 
                continue
            return frame
        raise ValueError

    def _serverGet(self, request, headers=None):
        """deferred to the result of GET /{request}"""
        _headers = {}
        if self.sendSourceLine:
            try:
                frame = self._callerFrame()
                _headers = {'x-source-line' :
                           "%s:%s" % (frame.f_code.co_filename, frame.f_lineno)}
            except ValueError:
                pass
        if headers is not None:
            _headers.update(headers)
        return getPage(self.serverUrl + request,
                       headers=_headers)

    def _checkQuerySyntax(self, query):
        if query == 'SELECT * WHERE { ?s ?p ?o. }':
            return
        if re.search(r"\?[a-z]+[;\.\]]", query):
            raise ValueError("sorry, sparqlhttp is currently too fragile for your syntax. Please change ' ?foo;' to ' ?foo ;' so sparqlhttp can find the variables more easily. This was your query:\n%s" % query)        

    def _getQuery(self, query, initBindings, headers=None):
        """send this query to the server, return deferred to the raw
        server result. This is where the prologue (@PREFIX lines) is added."""
        self._checkQuerySyntax(query)
        get = ('?query=' +
               urllib.quote(self.prologue +
                            interpolateSparql(query, initBindings), safe=''))

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
        if headers:
            sendHeaders.update(headers)
        
        d = self._serverGet(get, headers=sendHeaders)
        return d
        
    def remoteQueryd(self, query, initBindings={}):
        d = self._getQuery(query, initBindings)
        d.addCallback(parseSparqlResults)
        return d

    def remoteCountQuery(self, query, initBindings={}):
        # hint the server that it can return just a count (but if it
        # doesn't get the hint, we'll still count the result rows)
        d = self._getQuery(query, initBindings,
                           headers={'x-stat-result' : 'count'})
        
        @d.addCallback
        def checkType(result):
            tree = ElementTree.fromstring(result)
            # if it's pre-counted, return that
            try:
                return parseCountTree(tree)
            except ValueError:
                # server didn't get our hint, so it returned all the rows
                rows = parseSparqlResults(tree)
                return len(rows)
        return d

    def remoteAdd(self, *triples, **context):
        if 'context' not in context:
            raise TypeError("must pass 'context' kw arg")
        post = self.serverUrl + 'add?context=%s' % urllib.quote(context['context'], safe='')
        return self._postWithTriples(post, triples)

    def remoteSave(self, context):
        d = getPage(self.serverUrl + 'save?context=' + urllib.quote(context))
        return d

    def remoteContains(self, stmt):
        bindings = {}
        for termName, value in zip(['s', 'p', 'o'], stmt):
            if value is not None:
                bindings[Variable('?' + termName)] = value
            
        d = self.remoteCountQuery("SELECT * WHERE { ?s ?p ?o. }",
                                  initBindings=bindings)
        @d.addCallback
        def check(n):
            return n > 0
        return d

    def remoteLabel(self, subj, default=''):
        return self.remoteValue(subj, RDFS.label, default=default, any=True)

    def remoteValue(self, subj, pred, default=None, any=False):
        d = self.remoteQueryd("SELECT DISTINCT ?o WHERE { ?s ?p ?o }",
                              {Variable('?s') : subj, Variable('?p') : pred})

        @d.addCallback
        def justObject(rows):
            if len(rows) == 0:
                return default
            if len(rows) > 1 and not any:
                raise UniquenessError(values=[row['o'] for row in rows])
            return rows[0]['o']

        return d

    def remoteRemove(self, *triples, **context):
        context = context.get('context', None)
        post = self.serverUrl + 'remove'
        if context:
            post += '?context=%s' % urllib.quote(context, safe='')
        return self._postWithTriples(post, triples)
        
    def _postWithTriples(self, url, triples):
        g = graphFromTriples(triples)
        postData = g.serialize(format='nt')
        d = getPage(url, method='POST', postdata=postData)
        return d

                  

def graphFromTriples(triples):
    g = Graph()
    for stmt in triples:
        g.add(stmt)
    return g
            

class LocalGraph(object):
    """this is a local (in-process) graph, but with an API that's
    compatible with RemoteGraph (i.e. the remote* methods still return
    deferreds)"""
    def __init__(self, graph, initNs=None):
        assert initNs is None
        self.graph = graph

    def __getattr__(self, attr):
        """for methods like remoteLabel, call self.graph.label and
        wrap the result in a deferred"""
        if not attr.startswith("remote"):
            return self.__getattribute__(attr)
        
        def callLocalGraphMethod(*args, **kw):
            localVersion = (attr[len('remote')].lower() +
                            attr[len("remote") + 1:])
            result = getattr(self.graph, localVersion)(*args, **kw)
            return defer.succeed(result)
        return callLocalGraphMethod

        
def RemoteGraph(graph=None, serverUrl=None, **kw):
    """factory that returns a graph with methods like
    remoteLabel. But, you can choose if the graph is connected via
    http to a remote server, or if it's an in-process graph.

    g = RemoteGraph(graph=localGraphObj)
    g = RemoteGraph(serverUrl='http://localhost:9991/')

    """
    if serverUrl is not None:
        assert graph is None
        return _RemoteGraph(serverUrl, **kw)
    else:
        return LocalGraph(graph, **kw)
    

def interpolateSparql(query, initBindings):
    """expand the bindings into the query string to make one
    standalone query. Very sloppy; probably gets quoting wrong.

    >>> interpolateSparql('SELECT ?x { ?x ?y ?z }', {Variable('?z') : Literal('hi')})
    u'SELECT ?x { ?x ?y "hi" }'
    >>> interpolateSparql('SELECT ?x { ?x <http://example/?z=1> ?z }', {Variable('?z') : Literal('hi')})
    u'SELECT ?x { ?x <http://example/?z=1> "hi" }'
    """
    prolog = query[:query.find('{')]
    text = query[query.find('{'):]
    selection = sparqlSelection(query)
#    print "Sel is", selection
    for var, value in initBindings.items():
        if '?' + var not in selection:
            # hopefully you don't have spaces in your urls, and you do
            # have spaces on both sides of all variable names
            text = text.replace(' %s ' % var, ' %s ' % value.n3())
    query = prolog + text
#    print "Expand to", query
    return query 

if __name__ == '__main__':
    import doctest
    doctest.testmod()
