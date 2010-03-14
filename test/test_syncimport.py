import sys, os, traceback, logging
from twisted.trial import unittest
from twisted.internet import reactor, defer
import twisted.web
from rdflib import Namespace, Literal, RDFS, URIRef
from rdflib.Graph import Graph
sys.path.append("..")
from sparqlhttp.syncimport import SyncImport
from sparqlhttp.serve import SPARQLResource
from sparqlhttp.dictquery import Graph2

import shared
from shared import EXP

def writeFile(filename, txt):
    f = open(filename, "w")
    f.write(txt)
    f.close()

def afterOneSec(func):
    # I had to turn up the time a little, I think because file mtimes
    # are 1-sec resolution
    reactor.callLater(1.5, func)
    
class SyncTestCase(unittest.TestCase):
    def setUp(self):
        """all tests should return self.done"""
        logging.basicConfig(level=logging.INFO)

        try:
            os.remove("new.nt")
        except OSError:
            pass
        
        self.graph = Graph2(shared.localGraph(), initNs={'exp' : EXP})
        self.sync = SyncImport(self.graph, pollSeconds=.5)
        self.stmt1 = (EXP['dp'], EXP['name'], Literal("Drew"))

        self.done = defer.Deferred()
        self.done.addCallback(lambda result: self.sync.__del__())

    def checkAndComplete(self, checkFunc):
        """decorator to add some completion code after checkFunc"""
        def go():
            try:
                checkFunc()
                self.done.callback(None)
            except KeyboardInterrupt: raise
            except Exception, e:
                traceback.print_exc()
                self.done.errback(e)
                raise e
        return go

    def catchError(self, func):
        """decorator to make exceptions in func() abort the test"""
        def func2(*args, **kw):
            try:
                func(*args, **kw)
            except KeyboardInterrupt: raise
            except Exception, e:
                self.done.errback(e)
                raise e
        return func2

    def testSync(self):
        """input file appears and gets scanned"""
        self.assert_(not self.graph.contains(self.stmt1))
        writeFile("new.nt",
                '<http://example.org/dp> <http://example.org/name> "Drew" .\n')
            
        @afterOneSec
        @self.checkAndComplete
        def check():
            self.assert_(self.graph.contains(self.stmt1))
            
            inContext = self.graph.queryd(
                '''SELECT ?s WHERE {
                     GRAPH <http://example.org/new#context> {
                       ?s exp:name "Drew"
                     }
                   }''')
            self.assert_(list(inContext), [{'s' : EXP['dp']}])

        return self.done

    def testFormats(self):
        writeFile("new.n3", '''
@prefix : <http://example.org/> .
:dp :name "from n3" .
''')
        @afterOneSec
        @self.checkAndComplete
        def check():
            self.assert_(self.graph.contains(
                (EXP['dp'], EXP['name'], Literal("from n3"))))
        return self.done

    def testReplace(self):
        """input file is replaced with a new version

        this test is especially sensitive to the time in afterOneSec
        (1.0 sec is not always enough, given the resolution of file mtimes)
        """
        writeFile("new.nt",
                '<http://example.org/dp> <http://example.org/eye> "blue" .\n')
        @afterOneSec
        @self.catchError
        def check():
            self.assert_(self.graph.contains(
                (EXP['dp'], EXP['eye'], Literal('blue'))))
            writeFile("new.nt", '''\
<http://example.org/other> <http://example.org/eye> "other" .
<http://example.org/other2> <http://example.org/eye> "other2" .
''')
            @afterOneSec
            @self.checkAndComplete
            def check():
                #self.graph.dumpAllStatements()
                self.assert_(not self.graph.contains(
                    (EXP['dp'], EXP['eye'], Literal('green'))),
                             'stmt with object "green" is gone')
                self.assert_(self.graph.contains(
                    (EXP['other'], EXP['eye'], Literal('other'))),
                             'new stmt with object "other" is present')
            
        return self.done

    def testError(self):
        """good file is replaced with a bad file, and then another good one"""
        writeFile("new.nt",
                '<http://example.org/dp> <http://example.org/n> "one" .\n')
        @afterOneSec
        @self.catchError
        def check():
            self.assert_(self.graph.contains((EXP['dp'], EXP['n'],
                                              Literal("one"))),
                         "first good file puts a statement in the graph")

            writeFile("new.nt", '<corrupt or partial file')

            @afterOneSec
            @self.catchError
            def badFile():
                self.assert_(self.graph.contains(
                    (EXP['dp'], EXP['n'], Literal("one"))),
                             "original stmt stays in graph when file is bad")

                writeFile("new.nt",
                 '<http://example.org/dp> <http://example.org/n> "two" .\n')

                @afterOneSec
                @self.checkAndComplete
                def secondGoodFile():
                    self.assert_(not self.graph.contains((EXP['dp'], EXP['n'],
                                                          Literal("one"))),
                        "first statement is replaced with stmt from new file")
                    self.assert_(self.graph.contains(
                                     (EXP['dp'], EXP['n'], Literal("two"))),
                             "new file replaces the statement with a new one")

        return self.done
            
    def rdflib_bug_testOneLineNt(self):
        # no newline in file seems to reveal rdflib parse bug
        writeFile("new.nt",
                  '<http://example.org/dp> <http://example.org/name> "Drew" .')
        @afterOneSec
        @self.checkAndComplete
        def check():
            self.assert_(self.graph.contains(self.stmt1))

        return self.done
            
    def testLiteral(self):
        """making this work required a patch on 2007-02-05 in
        rdflib/syntax/parsers/ntriples.py"""
        writeFile("new.nt",
                '<http://example.org/dp> <http://example.org/date> "2007-02-05"^^<http://www.w3.org/2001/XMLSchema#date> .\n')
        @afterOneSec
        @self.checkAndComplete
        def check():
            self.assert_(self.graph.contains(
                (EXP['dp'], EXP['date'],
                 Literal('2007-02-05',
                         datatype=URIRef("http://www.w3.org/2001/XMLSchema#date")))))

        return self.done
