from __future__ import division
import traceback, time, logging, warnings
from twisted.web import http
from twisted.web.resource import Resource
from rdflib import Literal, URIRef
from rdflib.Graph import Graph
from sparqlhttp.sparqlxml import xmlResults, xmlCountResults
from sparqlhttp.stats import Stats

log = logging.getLogger("sparqlserve")

class SPARQLResource(Resource):
    """a sparql-over-http server. This resource is commonly at the
    root of an http server. There is ad-hoc support for some things
    beyond queries:
    POST /add?context={uri} with postdata that's an nt file
    GET /save?context={uri}
    GET / to view stats about what's been served
    POST /remove?context={uri} with postdata that's ntriples to remove, context is optional

    todo:
    DELETE /?context={uri}   drop this context
    """
    isLeaf = True
    def __init__(self, graph):
        self.graph = graph
        self.stats = Stats()

    def render_GET(self, request):
        # see http://twistedmatrix.com/projects/web/documentation/howto/using-twistedweb.html#rendering
        if request.path == '/':
            if 'query' not in request.args:
                return self.stats.statusPage()
            return self.getQuery(request)
        
        if request.path == '/save':
            warnings.warn('Use POST for /save requests', DeprecationWarning)
            return self.getSave(request)
        
        request.setResponseCode(http.BAD_REQUEST)
        return "<html>Invalid request: this is a sparql query server</html>"

    def getQuery(self, request):
        """GET /?query=SELECT... returns sparql results in xml"""
        query = request.args['query'][0]
        log.debug("received query: %r", query)
        isCount = request.getHeader('x-stat-result') == 'count'
        
        self.stats.queries += 1
        t1 = time.time()
        try:
            self.stats.lastQuery = query
            if isCount:
                count = self.graph.countQuery(query)
            else:
                results = self.graph.queryd(query)
        except Exception, e:
            self.stats.lastErrorQuery = query
            self.stats.lastError = traceback.format_exc()
            log.debug("query error: %s", self.stats.lastError)
            raise

        if not isCount:
            try:
                results = list(results)
                count = len(results)
                log.debug("got %s rows", count)
            except TypeError,e:
                count = 0 # this bogus value only affects the stats

        bindings = (request.getHeader('x-bindings') or "").split()
        queryKey = request.getHeader('x-uninterpolated-query-checksum') or query
        self.stats.ran(request.getHeader('x-source-line'),
                       query,
                       queryKey,
                       bindings,
                       time.time() - t1,
                       count)

        if isCount:
            return xmlCountResults(count)

        if request.getHeader('accept') == 'application/sparql-results+json':
            raise NotImplementedError
        else:
            # this one is failing, not sure why
            #ret = self.graph.queryd(query, format='xml')
            ret = xmlResults(results)
        return ret

    def getSave(self, request):
        """GET /save?context=http://example.org saves a context to a
        file (the file is local to this web server)"""
        ctx = URIRef(request.args['context'][0])
        self.graph.save(ctx)
        return "saved %s" % str(ctx)

    def render_POST(self, request):
        """you can post to
              /add?context=http://whatever
        with postdata that's an NT file, and that will add the
        statements to the given context.

        you can post to
            /remove?context=http://whatever
        with postdata in NT that will remove the statements from the
        context. Omit the context arg to remove from all contexts

        """
        if request.path == '/add':
            ctx = URIRef(request.args['context'][0])
            stmts = statementsFromNt(request.content)
            log.debug("add %s stmts to context %s", len(stmts), ctx)
            self.graph.add(*stmts, **dict(context=ctx))
            return "added to %s" % str(ctx)

        if request.path == '/remove':
            stmts = statementsFromNt(request.content)
            arg = request.args.get('context')
            if arg is not None:
                arg = URIRef(arg[0])
            log.debug("remove %s stmts from context=%s", len(stmts), arg)
            self.graph.remove(*stmts, **dict(context=arg))
            return "removed from context %s" % str(arg)

        if request.path == '/save':
            return self.getSave(request)

        request.setResponseCode(http.BAD_REQUEST)
        return "<html>unknown post path %r</html>" % request.postpath

def statementsFromNt(nt):
    g = Graph()
    g.parse(nt, format='nt')
    stmts = list(g)
    return stmts
