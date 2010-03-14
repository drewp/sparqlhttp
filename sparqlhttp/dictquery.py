from __future__ import division
import os, re, logging, time
import sys
import xml.sax

import rdflib
from rdflib import Variable as rdflib_Variable, RDFS, URIRef, BNode, Literal, StringInputSource
from rdflib.syntax.parsers.ntriples import ParseError
from rdflib.Graph import ConjunctiveGraph, Graph

if rdflib.__version__ == '2.4.0': # number might not be exactly right
    Variable = lambda v: rdflib_Variable('?' + v)
else:
    Variable = rdflib_Variable

assert map(int, rdflib.__version__.split('.')) >= [2,3,3], "requires rdflib 2.3.3 or newer"

try:
    from rdflib.sparql import Algebra
    # enable a plain SELECT query (with no graphs specified) to search all
    # data. Your add() calls will seem to have no effect unless you do
    # this. I'm not sure when it would be right to turn it off.
    Algebra.DAWG_DATASET_COMPLIANCE = False
except ImportError:
    pass # older rdflib, no rdflib.sparql.Algebra

log = logging.getLogger("Graph2")

# a better name for this might be SparqlMethodsGraph, since it uses
# sparql for all methods
class Graph2(object):
    """enhancements to the rdflib builtin graph. Only a few methods
    are implemented in this version

    The supported query operations (label, value) are all executed as
    sparql queries (using queryd), to make it easier to do them over a
    network

    New methods:
     save: like serialize but with automatic filename logic
     countQuery: like query but returns the row count
     queryd: like query but returns each row as a dict instead of tuple
     contains: same as __contains__ but renamed to make remoteContains easier

    Unstable methods:
     dumpAllStatements: print to console
     subgraphLength: len(graph.get_subgraph(s))
     subgraphClear: graph.get_subgraph(s).remove((None,None,None))
     safeParse: graph.parse(f,publicID=ctx,format=fmt), but no change to the graph if there's a parse error
     remove: requires a context

    there's a lot of near-repetition in this class and
    _RemoteGraph. Maybe this one should be written using the method
    bodies of LocalGraph, but with some trick to immediately pull the
    results from the deferreds. I.e. let RemoteGraph be the most full
    featured version, and then LocalGraph and Graph2 are the
    simplified cases.
    """
    
    def __init__(self, graph, initNs={}, savePrefix="http://", saveRootDir='.'):
        """initNs will be used on all queries

        savePrefix and saveRootDir configure where output files will
        be saved. See save() for details.
        """
        self.__dict__.update(vars())
        setupGetContextMethod(graph)

    def _graphModified(self):
        """this is called just before the graph gets modified, so you
        can clear your caches or whatever"""
        pass

    # this is weird for queryd to take a format arg. maybe i should
    # bring back query(), or make more variants for the other formats
    def queryd(self, query, initBindings=None, format='python'): 
        """like query, but yields dicts instead of tuples (that's
        what the final 'd' is for). ASK queries still return True/False

        The other query methods are implemented using this one."""

        if initBindings is None:
            initBindings = {}

        query, initBindings = fixDatatypedLiterals(query, initBindings)

        self._logQuery(query, initBindings)
        #query, initBindings = fixBNodes(query, initBindings)

        rows = self.graph.query(query, initBindings=initBindings,
                                initNs=self.initNs)
        if format != 'python':
            # or, use my sparqlxml.xmlResults() serializer
            return rows.serialize(format=format)
        rows = list(rows)
        log.debug("got %s rows" % len(rows))
        def returnIterator():
            # this implementation is dumb, but I didn't bother with a
            # better one because the real sparql query method already has
            # the right dict at some point. It just doesn't return the
            # dict.
            selection = sparqlSelection(query)
            for row in rows:
                yield dict(zip([x[1:] for x in selection], row))
        return returnIterator()


    def _logQuery(self, query, initBindings):
        if URIRef('http://projects.bigasterisk.com/2006/01/syncImport/lastImportTime') not in initBindings.values():
            log.debug("fixed to %r %r" % (query, initBindings))
        
    
    def countQuery(self, query, initBindings={}):
        # currently missing support for context, like queryd has
        n = 0
        for row in self.queryd(query, initBindings):
            n = n + 1
        return n

    def add(self, *triples, **context):
        """takes multiple triples at once (to reduce RPC calls).
        context arg is required"""
        self._graphModified() # why is this at the top, before the
                              # modification, instead of after the
                              # commit? need to check the usage
        try:
            context = context['context']
        except KeyError:
            raise TypeError("'context' named argument is required")
        subgraph = self.graph.get_context(context)
        for stmt in triples:
            subgraph.add(stmt)
            assert stmt in subgraph
            assert stmt in self.graph
        self.graph.commit()

    def save(self, context):
        """serialize the context to an already-determined filename.

        This context uri: {savePrefix}/{dir}/{dir}/{tail}#context

        writes to this output filename: {saveRootDir}/{dir}/{dir}/{tail}.nt
        
        """
        outPath = filenameFromContext(context, self.savePrefix,
                                      self.saveRootDir)
        try:
            os.makedirs(os.path.dirname(outPath))
        except OSError:
            pass # dirs exist

        
        self.graph.get_context(context).serialize(outPath, format="nt")

    def contains(self, stmt):
        # this should be an ASK, but ask seems to be messed up in rdflib
        binds = {}
        for var, node in zip(['s', 'p', 'o'], stmt):
            if node is not None:
                binds[Variable(var)] = node
        any = self.countQuery("SELECT * WHERE { ?s ?p ?o }",
                            initBindings=binds)
        return any > 0

    def label(self, subj, default=''):
        return self.value(subj, RDFS.label, default=default)

    def value(self, subj, pred, default=None):
        rows = iter(self.queryd("SELECT ?value WHERE { ?s ?p ?value }",
                                initBindings={Variable('s') : subj,
                                              Variable('p') : pred}))
        try:
            row = rows.next()
            return row['value']
        except StopIteration:
            return default

    def dumpAllStatements(self):
        print "Graph dump of %r:" % self
        print "ctx (subj, pred, obj)"
        print "-----------------------"
        for ctx in self.graph.contexts():
            for stmt in ctx:
                print ctx.identifier, stmt

    def subgraphLength(self, ctx):
        """same as len(graph.get_context(ctx)),
        since this class doesn't have get_context"""
        return len(self.graph.get_context(ctx))

    def subgraphClear(self, ctx):
        """same as graph.get_context(ctx).remove((None, None, None))"""
        self._graphModified()
        self.graph.get_context(ctx).remove((None, None, None))

    def safeParse(self, source, publicID=None, format="xml"):
        if isinstance(source, StringInputSource):
            # StringIO won't parse twice, which is the approach used
            # below. Once that's fixed to parse only once, this block
            # is not needed. See the other block below too
            saveTxt = source.getByteStream().read()
            source = StringInputSource(saveTxt)
        
        # the goal is to avoid clearing the ctx unless we know
        # the new file parses ok. For now I do the parse
        # twice, but if that slowdown is annoying (or if it's
        # a problem that the file could change between
        # parses), this can be rewritten to parse into a new
        # graph and then copy to the main graph. Or parse into
        # a new context and then rename the ctxs.
        try:
            Graph().parse(source, publicID=URIRef('http://example.org/'), format=format)
        except (xml.sax._exceptions.SAXParseException,
                ParseError):
            log.warn("parse error reading file. context is empty or incomplete")
            raise

        self._graphModified()

        log.debug("clear old %s", publicID)
        subgraph = self.graph.get_context(publicID)
        subgraph.remove((None, None, None))
        

        if isinstance(source, StringInputSource):
            source = StringInputSource(saveTxt)

        log.info("parse %s into %s", source, publicID)
        t1 = time.time()
        self.graph.parse(source, publicID=publicID, format=format)
        now = time.time()
        # hey- this logging could be expensive:
        log.info("parsed %s stmt in %.1f sec (%.1f sps)" % (
            len(subgraph), now - t1,
            len(subgraph) / (now - t1)))
        self.graph.commit()

    def remove(self, *triples, **context):
        """graph.get_context(context).remove(stmt)"""
        self._graphModified()
        context = context.get('context')
        if context is None:
            g = self.graph
        else:
            g = self.graph.get_context(context)
        for triple in triples:
            g.remove(triple)

def fixDatatypedLiterals(query, initBindings):
    """workaround for sparql parser bug:

       If you say something like
         "2007-01-24"^^<http://www.w3.org/2001/XMLSchema#date>
       it errors on 'not an accepted datatype'.

       Here's the workaround, which sticks more ?vars in your query
       and moves those literals into the initBindings. This:

         SELECT ?x WHERE { ?x ?y "2007-01-24"^^<http://www.w3.org/2001/XMLSchema#date> }

       becomes this:

         SELECT ?x WHERE { ?x ?y ?fix0 }
         initBindings: fix0 -> Literal("2007-01-24", datatype=URIRef("http://www.w3.org/2001/XMLSchema#date"))

    """
    count = [0]

    def repl(match):
        value, datatype = match.groups()
        var = 'fix%s' % count[0]
        count[0] = count[0] + 1
        initBindings[var] = Literal(value, datatype=URIRef(datatype))
        return '?'+var
        
    query = re.sub(r'"(.*?)"\^\^<([^>]+)>', repl, query)
    
    return query, initBindings


## # i think this is a bad idea
## def fixBNodes(query, initBindings):
##     """
##     bnodes get interpolated into the sparql like this:
##        _:ImucpKgE40 :pred :obj .

##     but the sparql parser is incorrectly erroring on the '_'
##     prefix. Same workaround as fixDatatypedLiterals: convert the
##     bnodes to initBindings.
##     """
##     count = [0]
    
##     def repl(match):
##         id, = match.groups()
##         var = '?bnode%s' % count[0]
##         count[0] = count[0] + 1
##         initBindings[var] = BNode(
        
##     query = re.sub(r'_:(\w+)', repl, query)
    
##     return query, initBindings

def sparqlSelection(query):
    assert ' WHERE ' in query, "sorry, I require the WHERE token because of some dumb parsing"
    return [w for w in query.split("WHERE", 1)[0].split("SELECT",1)[1].split()
            if w.startswith("?")]

        
def setupGetContextMethod(graph):
    # why can't i have get_context from old BackwardCompatGraph? unclear
    # how that should be done in ConjunctiveGraph
    def get_context(self, identifier, quoted=False):
        """Return a context graph for the given identifier

        identifier must be a URIRef or BNode.
        """
        assert isinstance(identifier, URIRef) or \
               isinstance(identifier, BNode), type(identifier)
        if quoted:
            assert False
            return QuotedGraph(self.store, identifier)
            #return QuotedGraph(self.store, Graph(store=self.store,
            #                                     identifier=identifier))
        else:
            return Graph(store=self.store, identifier=identifier,
                         namespace_manager=self)
            #return Graph(self.store, Graph(store=self.store,
            #                               identifier=identifier))


    import types
    graph.get_context = types.MethodType(get_context, graph, graph.__class__)

def filenameFromContext(context, prefix, rootDir):
    if not context.startswith(prefix):
        raise ValueError("can't save context %s, it doesn't start with %s"
                         % (context, prefix))
    assert context.endswith("#context") # rdflib requirement
    innerPath = context[len(prefix):-len("#context")]
    return os.path.join(rootDir, innerPath + ".nt")

def contextFromFilename(filename, prefix, rootDir, allowedExtensions=['.nt', '.n3']):
    """
    prefix will get a trailing / if it doesn't already have
    one. Trailing / is ignored on rootDir.
    """
    prefix = prefix.rstrip('/') + '/'
    if not filename.startswith(rootDir):
        raise ValueError("filename %r is not in %r" % (filename, rootDir))
    base, ext = os.path.splitext(filename)
    if ext not in allowedExtensions:
        raise ValueError("filename %r has an unknown extension" % filename)
    relPath = base[len(rootDir.rstrip('/')):]
    return URIRef("%s%s#context" % (prefix, relPath.lstrip('/')))


