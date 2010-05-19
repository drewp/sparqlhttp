"""
more tests needed:
  multiple appearances of a bnode in an add don't become multiple bnodes
  simultaneous queries?
  more error cases
"""

import os, sys, logging
from twisted.trial import unittest
from twisted.internet import reactor
import twisted.web
from rdflib import Namespace, Literal, RDFS, BNode, Variable, URIRef
from rdflib.Graph import Graph
from rdflib.exceptions import UniquenessError
import rdflib
print rdflib

sys.path.append("..")
from sparqlhttp.remotegraph import RemoteGraph
from sparqlhttp.serve import SPARQLResource
from sparqlhttp.dictquery import Graph2

import shared
from shared import EXP, QUERY

logging.basicConfig(level=logging.INFO)
logging.info("rdflib version %s" % rdflib.__version__)

class Cases(object):
    """these are closely based on the simple cases in
    test_dictquery.py, but they're all rewritten to use deferreds."""
    def testQuerydForm1(self):
        d = self.graph.remoteQueryd(QUERY.interpolated)
        @d.addCallback
        def check(rows):
            self.assertEqual(list(rows), QUERY.result)
        return d

    def testQuerydForm2(self):
        d = self.graph.remoteQueryd(QUERY.name, initBindings=QUERY.nameBindings)
        @d.addCallback
        def check(rows):
            self.assertEqual(list(rows), QUERY.result)
        return d

    def testQuerydForm3(self):
        d = self.graph.remoteQueryd(QUERY.prefixedNames)
        @d.addCallback
        def check(rows):
            self.assertEqual(list(rows), QUERY.result)
        return d

    def xxx_testQuerydEmptyNamespace(self):
        d = self.graph.remoteQueryd(QUERY.emptyPrefixNames)
        @d.addCallback
        def check(rows):
            self.assertEqual(list(rows), QUERY.result)
        return d

    def xxxtestQuerydSyntaxError(self):
        d = self.graph.remoteQueryd("nonsense text")
        @d.addErrback
        def check(err):
            # check the error type here?
            self.assert_(err)
        @d.addCallback
        def bad(result):
            self.assert_(False)
        return d

    def xxxtestQuerydSyntaxError2(self):
        d = self.graph.remoteQueryd(QUERY.syntaxError)
        @d.addErrback
        def check(err):
            self.assert_(err)
        @d.addCallback
        def bad(result):
            self.assert_(False)
        return d

    
    def testQueryContext(self):
        ctx2 = EXP['ctx2#context']
        d = self.graph.remoteAdd((EXP['dp'], EXP['firstName'],
                                  Literal("othercontext")),
                                 context=ctx2)
        @d.addCallback
        def withTwoContexts(result):
            return self.graph.remoteQueryd(QUERY.interpolated)
        @d.addCallback
        def check(rows):
            self.assertEqual(len(list(rows)), 2)

            return self.graph.remoteQueryd(QUERY.withContext)
        @d.addCallback
        def check2(rows):
            self.assertEqual(list(rows), [{'name' : Literal("othercontext")}])
        return d

    def testQueryDatatype(self):
        """my own patched rdflib gets this right; stock 2.4.0 gets it
        wrong. I think the issue is Literal.py and the comparison
        functions"""
        date = Literal("2007-02-05",
                    datatype=URIRef("http://www.w3.org/2001/XMLSchema#date"))
        d = self.graph.remoteAdd((EXP['c'], EXP['d'], date),
                                 context=EXP['ctx#context'])
        @d.addCallback
        def doQuery(_):
            return self.graph.remoteQueryd('SELECT ?c WHERE { ?c exp:d "2007-02-05"^^<http://www.w3.org/2001/XMLSchema#date> }')
        @d.addCallback
        def check(rows):
            self.assertEqual(list(rows), [{'c': EXP['c']}]) # known issue in rdflib 2.4.0
        return d

    def testPreserveOptionalKeysInResult(self):
        d = self.graph.remoteQueryd('SELECT ?first ?last WHERE { ?x exp:firstName ?first . OPTIONAL { ?x exp:lastName ?last . } }')
        @d.addCallback
        def check(rows):
            self.assertEqual(list(rows), [{'first' : Literal('Drew'),
                                           'last' : None}])
        return d
        
    def testCountQuery(self):
        d = self.graph.remoteCountQuery(QUERY.name,
                                        initBindings=QUERY.nameBindings)
        @d.addCallback
        def check(n):
            self.assertEqual(n, 1)
        return d

    def testContainsNegative(self):
        d = self.graph.remoteContains(shared.nonexistStatement)
        @d.addCallback
        def check(contains):
            self.assert_(not contains)
        return d

    def testContainsPositive(self):
        d = self.graph.remoteContains(shared.existStatement)
        @d.addCallback
        def check(contains):
            self.assert_(contains)
        return d
     
    def testAdd(self):
        s = shared.newStatement

        self.assertRaises(TypeError, self.graph.remoteAdd, s)

        d = self.graph.remoteContains(s)
        @d.addCallback
        def check(contains):
            self.assert_(not contains, "stmt already in graph!")

        @d.addCallback
        def doAdd(result):
            return self.graph.remoteAdd(s, context=EXP['ctx'])

        @d.addCallback
        def checkNow(result):
            return self.graph.remoteContains(s)
        
        @d.addCallback
        def check(contains):
            self.assert_(contains, "stmt didn't appear in graph after add")
        return d

    def testAddLiteral(self):
        date = Literal("2007-02-05",
                    datatype=URIRef("http://www.w3.org/2001/XMLSchema#date"))
        d = self.graph.remoteAdd((EXP['a'], EXP['b'], Literal("notype")),
                                 context=EXP['ctx#context'])
        @d.addCallback
        def contains(add):
            return self.graph.remoteContains((EXP['a'], EXP['b'], Literal("notype")))
        @d.addCallback
        def check(contains):
            self.assert_(contains)
            return self.graph.remoteAdd((EXP['c'], EXP['d'], date),
                                        context=EXP['ctx#context'])
        @d.addCallback
        def add(_):
            return self.graph.remoteContains((EXP['c'], EXP['d'], date))
        @d.addCallback
        def check(contains):
            self.assert_(contains)
            
        return d

    def testAddMultiple(self):
        d = self.graph.remoteAdd(*shared.newStatements,
                                 **dict(context=EXP['ctx#context']))

        for stmt in shared.newStatements:
            @d.addCallback
            def check(result):
                return self.graph.remoteContains(stmt)
            @d.addCallback
            def check(contains):
                self.assert_(contains)
        return d

    def testSave(self):
        d = self.graph.remoteAdd(*shared.newStatements,
                                 **dict(context=EXP['ctx#context']))
        outFile = "example.org/ctx.nt"
        try:
            os.remove(outFile)
        except OSError:
            pass
        @d.addCallback
        def then(result):
            self.assert_(not os.path.exists(outFile))
            return self.graph.remoteSave(EXP['ctx#context'])
        @d.addCallback
        def check(result):
            self.assert_(os.path.exists(outFile))
            # do it again, now that the dirs exist
            return self.graph.remoteSave(EXP['ctx#context'])
        return d

    def testLabelNegative(self):
        d = self.graph.remoteLabel(EXP['nonexist'])
        @d.addCallback
        def check(result):
            self.assertEqual(result, '')
        return d

    def testLabelDefault(self):
        d = self.graph.remoteLabel(EXP['nonexist'], default="foo")
        @d.addCallback
        def check(result):
            self.assertEqual(result, 'foo')
        return d

    def testLabelPositive(self):
        d = self.graph.remoteLabel(EXP['labeled'])
        @d.addCallback
        def check(result):
            self.assertEqual(result, Literal('Labeled'))
        return d

    def testLabelMultiple(self):
        d = self.graph.remoteAdd((EXP['labeled'], RDFS.label,
                                  Literal("labeledtwice")),
                                 context=EXP['ctx#context'])
        @d.addCallback
        def go(result):
            return self.graph.remoteLabel(EXP['labeled'])
        @d.addErrback
        def uniqueness(err):
            print "good", err
            self.assert_(isinstance(err.value, UniquenessError))

            return self.graph.remoteLabel(EXP['labeled'], any=True)
        @d.addCallback
        def check(label):
            self.assert_(label in [Literal("Labeled"), Literal("labeledtwice")])
        return d
    
    def testValue1(self):
        d = self.graph.remoteValue(EXP['dp'], EXP['firstName'])
        @d.addCallback
        def check(name):
            self.assertEqual(name, Literal('Drew'))
        return d

    def testValue2(self):
        d = self.graph.remoteValue(EXP['dp'], EXP['nonexist'])
        @d.addCallback
        def check(value):
            self.assertEqual(value, None)
        return d

    def testRemove(self):
        # still missing a test of remoteRemove with context arg
        s0 = (EXP['x'], EXP['name0'], Literal("a"))
        s1 = (EXP['x'], EXP['name1'], Literal("b"))
        d = self.graph.remoteAdd(s0, s1, context=EXP['ctx#context'])
        @d.addCallback
        def rm(result):
            d = self.graph.remoteRemove(s0)
            d.addCallback(lambda result:
                          self.graph.remoteQueryd("SELECT ?p WHERE { exp:x ?p ?o }"))
            return d
        @d.addCallback
        def check2(result):
            self.assertEqual(list(result), [{'p' : EXP['name1']}])
        return d

##     def testBNode(self):
##         b = BNode()
##         d = self.graph.remoteAdd((b, EXP['label'], Literal("bnode1")),
##                                  context=EXP['ctx#context'])
##         @d.addCallback
##         def query(result):
##             return self.graph.remoteQueryd(
##                 "SELECT ?label WHERE { ?bn exp:label ?label }",
##                 initBindings={Variable("?bn") : b})
##         @d.addCallback
##         def check(rows):
##             self.assertEqual(list(rows), [{'label' : 'bnode1'}])

##         return d
        


        

class LocalTestCase(unittest.TestCase, Cases):
    def setUp(self):
        localGraph = Graph2(shared.localGraph(), initNs={'exp' : EXP, '' : EXP})
        self.graph = RemoteGraph(graph=localGraph)

class RemoteTestCase(unittest.TestCase, Cases):
    def setUp(self):
        localGraph = Graph2(shared.localGraph(), initNs={'exp' : EXP, '' : EXP})
        self.listen = reactor.listenTCP(9991,
                     twisted.web.server.Site(SPARQLResource(localGraph)))
        self.graph = RemoteGraph(serverUrl="http://localhost:9991/",
                                 resultFormat='xml')

    def tearDown(self):
        self.listen.stopListening()
