from __future__ import division
import traceback, time
from twisted.web import http
from twisted.web.resource import Resource
from rdflib import Literal, URIRef
from rdflib.Graph import Graph
from sparqlhttp.sparqlxml import xmlResults, xmlCountResults
from sparqlhttp.stats import Stats

class SPARQLResource(Resource):
    """a sparql-over-http server. This resource is commonly at the
    root of an http server. There is ad-hoc support for some things
    beyond queries:
    POST /add?context={uri} with postdata that's an nt file
    GET /save?context={uri}
    GET / to view stats about what's been served
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
            return self.getSave(request)
        
        request.setResponseCode(http.BAD_REQUEST)
        return "<html>Invalid request: this is a sparql query server</html>"

    def getQuery(self, request):
        """GET /?query=SELECT... returns sparql results in xml"""
        query = request.args['query'][0]
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
            raise

        if not isCount:
            try:
                results = list(results)
                count = len(results)
            except TypeError,e:
                count = 0 # this bogus value only affects the stats

        self.stats.ran(request.getHeader('x-source-line'),
                       query,
                       request.getHeader('x-uninterpolated-query-checksum'),
                       request.getHeader('x-bindings').split(),
                       time.time() - t1,
                       count)

        if isCount:
            return xmlCountResults(count)

        # this one is failing, not sure why
        #ret = self.graph.queryd(query, format='xml')
        ret = xmlResults(results)
        return ret

    def getSave(self, request):
        """GET /save?context=http://example.org saves a context to a
        file (the file is local to this web server)"""
        ctx = URIRef(request.args['context'][0])
        self.graph.save(ctx)
        return "ok"

    def render_POST(self, request):
        """you can post to
              /add?context=http://whatever
        with postdata that's an NT file, and that will add the
        statements to the given context"""
        if request.path == '/add':
            ctx = URIRef(request.args['context'][0])
            adds = Graph()
            adds.parse(request.content, format='nt')
            stmts = list(adds)
            self.graph.add(*stmts, **dict(context=ctx))
            return "ok"

        request.setResponseCode(http.BAD_REQUEST)
        return "<html>unknown post path %r</html>" % request.postpath
