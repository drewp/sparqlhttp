"""
many pieces of data are shared between the local testing in
test_dictquery.py and the remote testing in test_remotegraph.py

Their function names and async forms are all different, though, so
they can't share much code. Just data.
"""
from rdflib import Namespace, Literal, Variable, RDFS
from rdflib.Graph import ConjunctiveGraph

EXP = Namespace("http://example.org/")
XS = Namespace("http://www.w3.org/2001/XMLSchema#")

def localGraph():
    g = ConjunctiveGraph()
    g.add((EXP['dp'], EXP['firstName'], Literal('Drew')))
    g.add((EXP['labeled'], RDFS.label, Literal('Labeled')))
    return g


existStatement = (EXP['dp'], EXP['firstName'], Literal('Drew'))
nonexistStatement = (EXP['nonexist'],EXP['nonexist'],EXP['nonexist'])
newStatement = (EXP['new'], EXP['firstName'], Literal('newname'))
newStatements = [(EXP['new2'], EXP['p'], EXP['o2']),
                 (EXP['new3'], EXP['p'], EXP['o3'])]

class QUERY:
    name = "SELECT ?name WHERE { ?subj ?pred ?name }"
    nameBindings = {Variable('?subj') : EXP['dp'],
                    Variable('?pred') : EXP['firstName']}
    interpolated = """SELECT ?name WHERE {
             <http://example.org/dp> <http://example.org/firstName> ?name }"""
    withContext = """SELECT ?name WHERE {
             GRAPH <http://example.org/ctx2#context> { <http://example.org/dp> <http://example.org/firstName> ?name } }"""
    prefixedNames = "SELECT ?name WHERE { exp:dp exp:firstName ?name }"
    emptyPrefixNames = "SELECT ?name WHERE { :dp :firstName ?name }"
    result = [{'name' : Literal('Drew')}]

    # this one starts ok, so it'll pass some of the parsing, but then it breaks
    syntaxError = "SELECT ?foo WHERE { syntax error"
