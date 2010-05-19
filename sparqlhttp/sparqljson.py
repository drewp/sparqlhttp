import jsonlib
from rdflib import Literal, URIRef

def parseJsonResults(jsonResults):
    """returns the same as parseSparqlResults. Takes json string like this:

    { 'head': { 'link': [], 'vars': ['p', 'o'] },
      'results': { 'distinct': false, 'ordered': true, 'bindings': [
        { 'p': { 'type': 'uri',
                 'value': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' },
          'o': { 'type': 'uri',
                 'value': 'http://fantasyfamegame.com/2006/01/User' }},
        { 'p': { 'type': 'uri',
                 'value': 'http://fantasyfamegame.com/2006/01/username' },
        'o': { 'type': 'literal', 'value': 'drewp' }},
        { 'p': { 'type': 'uri', 'value': 'http://fantasyfamegame.com/2006/01/passwordSHA' }	, 'o': { 'type': 'literal', 'value': '23fa12c6b4e9e3805a5e9d5dded3e78665fc1899' }},
      ...
    """

    ret = []
    for row in jsonlib.loads(jsonResults)['results']['bindings']:
        outRow = {}
        for k, v in row.items():
            outRow[k] = parseJsonTerm(v)
        ret.append(outRow)
    return ret


def jsonRowCount(jsonResults):
    """given a json string like parseJsonTerm takes, just count the rows"""
    return len(jsonlib.loads(jsonResults)['results']['bindings'])
    
def parseJsonTerm(d):
    """rdflib object (Literal, URIRef, BNode) for the given json-format dict.
    
    input is like:
      { 'type': 'uri', 'value': 'http://famegame.com/2006/01/username' }
      { 'type': 'literal', 'value': 'drewp' }
    """
    # this implementation is purely a guess. i haven't looked up the spec yet.
    
    t = d['type']
    if t == 'uri':
        return URIRef(d['value'])
    elif t == 'literal':
        return Literal(d['value'])
    elif t == 'typed-literal':
        return Literal(d['value'], datatype=URIRef(d['datatype']))
    else:
        raise NotImplementedError("json term type %r" % t)
