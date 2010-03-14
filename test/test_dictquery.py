import sys, os, logging

from rdflib import Literal, Variable, RDFS, StringInputSource
from rdflib.syntax.parsers.ntriples import ParseError
import rdflib
from twisted.trial import unittest
sys.path.append("..")
from sparqlhttp.dictquery import Graph2

from shared import EXP, QUERY, XS
import shared

log = logging.basicConfig(level=logging.DEBUG)
print "rdflib", rdflib

class TestCase(unittest.TestCase):
    def setUp(self):
        self.graph = Graph2(shared.localGraph(), initNs={'exp' : EXP, '' : EXP})
        
    def testQueryd(self):
        rows = self.graph.queryd(QUERY.interpolated)
        self.assertEqual(list(rows), QUERY.result)

        rows = self.graph.queryd(QUERY.name, initBindings=QUERY.nameBindings)
        self.assertEqual(list(rows), QUERY.result)

        rows = self.graph.queryd(QUERY.prefixedNames)
        self.assertEqual(list(rows), QUERY.result)

    def rdflib_broken_testQuerydEmptyNamespace(self):
        rows = self.graph.queryd(QUERY.emptyPrefixNames)
        self.assertEqual(list(rows), QUERY.result)

    def rdflib_broken_testQuerydSyntaxError(self):
        self.assertRaises(ValueError, self.graph.queryd, "nonsense text")
        self.assertRaises(ValueError, self.graph.queryd, QUERY.syntaxError)

    def testQueryContext(self):
        ctx2 = EXP['ctx2#context']
        self.graph.add((EXP['dp'], EXP['firstName'], Literal("othercontext")),
                       context=ctx2)
        rows = self.graph.queryd(QUERY.interpolated)
        self.assertEqual(len(list(rows)), 2)

        rows = self.graph.queryd(QUERY.withContext)
        self.assertEqual(list(rows), [{'name' : Literal("othercontext")}])
        

    def testCountQuery(self):
        n = self.graph.countQuery(QUERY.name, initBindings=QUERY.nameBindings)
        self.assertEqual(n, 1)

    def testContains(self):
        self.assert_(not self.graph.contains(shared.nonexistStatement))
        self.assert_(self.graph.contains(shared.existStatement))
        
    def testAdd(self):
        s = shared.newStatement
        self.assert_(not self.graph.contains(s))
        self.assertRaises(TypeError, self.graph.add, s)
        self.graph.add(s, context=EXP['ctx'])
        self.assert_(self.graph.contains(s))

    def testAddMultiple(self):
        self.graph.add(*shared.newStatements,
                       **dict(context=EXP['ctx']))
        for stmt in shared.newStatements:
            self.assert_(self.graph.contains(stmt))

    def testSave(self):
        self.graph.add(*shared.newStatements,
                       **dict(context=EXP['ctx#context']))
        outFile = "example.org/ctx.nt"
        self.assert_(not os.path.exists(outFile))
        self.graph.save(EXP['ctx#context'])
        self.assert_(os.path.exists(outFile))
        # again, now that the dirs exist
        self.graph.save(EXP['ctx#context'])

    def testSaveOneContext(self):
        ctx2 = EXP['ctx2#context']
        self.graph.add(shared.newStatement, context=ctx2)
        self.graph.save(ctx2)
        self.assertEqual(len(open("example.org/ctx2.nt").read().splitlines()), 1)
        

    def testLabel(self):
        self.assertEqual(self.graph.label(EXP['nonexist']), '')
        self.assertEqual(self.graph.label(EXP['labeled']), Literal('Labeled'))
        self.graph.add((EXP['labeled'], RDFS.label, Literal("labeledtwice")),
                       context=EXP['ctx#context'])
        self.assert_(self.graph.label(EXP['labeled']) in [Literal("Labeled"), Literal("labeledtwice")])

    def testValue(self):
        name = self.graph.value(EXP['dp'], EXP['firstName'])
        self.assertEqual(name, Literal('Drew'))
        self.assertEqual(self.graph.value(EXP['dp'], EXP['nonexist']), None)
        
        self.graph.add((EXP['dp'], EXP['firstMame'], EXP['anotherPred']),
                       context=EXP['ctx'])

    def testXsBoolean(self):
        xsTrue = Literal("true", datatype=XS['boolean'])
        self.graph.add((EXP['labeled'], EXP['state'], xsTrue),
                       context=EXP['ctx#context'])
        oneResult = [{'x' : EXP['labeled']}]
        
        rows = self.graph.queryd('SELECT ?x WHERE { ?x exp:state ?true }',
                                 initBindings={Variable("?true") : xsTrue})
        self.assertEqual(list(rows), oneResult)

        rows = self.graph.queryd('SELECT ?x WHERE { ?x exp:state "true"^^<http://www.w3.org/2001/XMLSchema#boolean> }')
        self.assertEqual(list(rows), oneResult)

        rows = self.graph.queryd('SELECT ?x WHERE { ?x exp:state ?false }',
                                 initBindings={Variable("?false") :
                                    Literal("false", datatype=XS['boolean'])})
        self.assertEqual(list(rows), [])

    def testXsDate(self):
        date = Literal("2007-01-27", datatype=XS['date'])
        self.graph.add((EXP['labeled'], EXP['date'], date),
                       context=EXP['ctx#context'])

        from rdflib.sparql.bison import Parse
        q = Parse('SELECT ?x ?y WHERE { ?x exp:date ?y }')
        print q
        rows = list(self.graph.queryd('SELECT ?x ?y WHERE { ?x exp:date ?y }'))
        print rows

        # sparql query looks in self.graph.graph.default_context.triples
        print list(self.graph.graph.triples((None, EXP['date'], None)))

        
        return
        #######################################################################
        rows1 = list(self.graph.queryd('SELECT ?x WHERE { ?x exp:date "2007-01-27"^^<http://www.w3.org/2001/XMLSchema#date> }'))
        print "rows1", rows1

        rows2 = list(self.graph.queryd('SELECT ?x WHERE { ?x exp:date "2001-01-11"^^<http://www.w3.org/2001/XMLSchema#date> }'))
        print "rows2", rows2

        rows3 = list(self.graph.queryd('SELECT ?x WHERE { ?x exp:date ?d }',
                                       initBindings={Variable("?d") :
                                                     rows[0]['y']}))#Literal("2007-01-27", datatype=URIRef("http://www.w3.org/2001/XMLSchema#date"))}))
        print "rows3", rows3

        oneResult = [{'x' : EXP['labeled']}]
        self.assertEqual(rows1, oneResult)
        self.assertEqual(rows2, [])
        self.assertEqual(rows3, oneResult)
       

    def testUnion(self):
        self.graph.add((EXP['x'], EXP['name0'], Literal("eks")),
                       (EXP['x'], EXP['name1'], Literal("Eks")),
                       (EXP['x'], EXP['state'], Literal("on")),
                       
                       (EXP['y'], EXP['name0'], Literal("why")),
                       (EXP['y'], EXP['name1'], Literal("Why")),
                       
                       context=EXP['ctx#context'])
        
        rows = list(self.graph.queryd(
            '''SELECT ?letter ?name WHERE {
                 {
                   ?letter exp:name0 ?name .
                   ?letter exp:state "on" .
                 } UNION {
                   ?letter exp:name1 ?name .
                   ?letter exp:state "on" .
                 }
               }'''))
        self.assertEqual(len(rows), 2)
        
    def testSubgraphLength(self):
        self.graph.add((EXP['x'], EXP['name0'], Literal("eks")),
                       (EXP['x'], EXP['name1'], Literal("Eks")),
                       (EXP['x'], EXP['state'], Literal("on")),
                       context=EXP['ctxnew#context'])
        self.assertEqual(self.graph.subgraphLength(EXP['ctxnew#context']), 3)
        self.assertEqual(self.graph.subgraphLength(EXP['ctxunused#context']), 0)
        
    def testSubgraphClear(self):
        self.graph.add((EXP['x'], EXP['name0'], Literal("eks")),
                       (EXP['x'], EXP['name1'], Literal("Eks")),
                       (EXP['x'], EXP['state'], Literal("on")),
                       context=EXP['ctxnew#context'])
        self.graph.subgraphClear(EXP['ctxnew#context'])
        self.assertEqual(self.graph.subgraphLength(EXP['ctxnew#context']), 0)

    def testSafeParse(self):
        s = StringInputSource('''\
<http://example.org/x> <http://example.org/name0> "eks" .
<http://example.org/x> <http://example.org/name1> "Eks" .
''')
        self.graph.safeParse(s, publicID=EXP['ctxnew#context'], format='nt')
        self.assert_(self.graph.contains((EXP['x'], EXP['name0'],
                                          Literal("eks"))),
                     "new statement read in")
        
        err = StringInputSource('''\
<http://examplcorrupted''')
        
        self.assertRaises(ParseError,
                          self.graph.safeParse,
                          err, publicID=EXP['ctxnew#context'], format='nt')

        self.assert_(self.graph.contains((EXP['x'], EXP['name0'],
                                          Literal("eks"))),
                     "old statement still in graph")

        ok = StringInputSource('''\
<http://example.org/x> <http://example.org/name0> "new" .
''')
        self.graph.safeParse(ok, publicID=EXP['ctxnew#context'], format='nt')
        self.assert_(not self.graph.contains((EXP['x'], EXP['name0'],
                                              Literal("eks"))),
                     "old statement removed from graph")
        self.assert_(self.graph.contains((EXP['x'], EXP['name0'],
                                          Literal("new"))),
                     "new statement got in graph")

        n3 = StringInputSource('''
@prefix : <http://example.org/> .
:dp :name "from n3" .
''')
        self.graph.safeParse(n3, publicID=EXP['ctxn3#context'], format='n3')
        self.assert_(self.graph.contains((EXP['dp'], EXP['name'],
                                          Literal("from n3"))))
    def testRemove(self):
        s0 = (EXP['x'], EXP['name0'], Literal("a"))
        s1 = (EXP['x'], EXP['name1'], Literal("b"))
        s2 = (EXP['x'], EXP['name2'], Literal("c"))

        self.graph.add(s0, s1, s2, context=EXP['ctx#context'])

        self.graph.remove(s0, context=EXP['ctx#context'])
        self.assert_(not self.graph.contains(s0))

        self.graph.remove(s1)
        self.assert_(not self.graph.contains(s1))
