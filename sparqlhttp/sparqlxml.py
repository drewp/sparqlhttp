from nevow import flat, tags as T, stan, json
from nevow.stan import Tag
from rdflib import URIRef, Literal, BNode
try:
    from xml.etree import cElementTree as ElementTree
except ImportError:
    from elementtree import ElementTree
"""
see http://www.w3.org/TR/rdf-sparql-XMLres/


Todo:
- literal language support (currently ignored)
"""

RESULTS_NS = "http://www.w3.org/2005/sparql-results#"
EXTENDED_NS = "http://projects.bigasterisk.com/2006/01/sparqlExtended/"
RESULTS_NS_ET = '{%s}' % RESULTS_NS

def term(t):
    """stan xml node for the given rdflib term"""
    if isinstance(t, URIRef):
        return Tag("uri")[t]
    elif isinstance(t, Literal):
        ret = Tag("literal")[t]
        if t.datatype is not None:
            ret.attributes['datatype'] = t.datatype
        return ret
    elif isinstance(t, BNode):
        return Tag("bnode")[t]
    else:
        raise TypeError("unknown term type %r" % t)
#        return Tag("literal")['<none>']

def parseTerm(element):
    """rdflib object (Literal, URIRef, BNode) for the given
    elementtree element"""
    tag, text = element.tag, element.text
    if tag == RESULTS_NS_ET + 'literal':
        if text is None:
            text = ''
        ret = Literal(text)
        if element.get('datatype', None):
            ret.datatype = URIRef(element.get('datatype'))
        return ret
    elif tag == RESULTS_NS_ET + 'uri':
        return URIRef(text)
    elif tag == RESULTS_NS_ET + 'bnode':
        return BNode(text)
    else:
        raise TypeError("unknown binding type %r" % element)

def xmlResults(resultRows):
    """xml text for a list of sparql results. These rows are a list of
    dicts with variable names as keys, rdflib objects as values.

    Values of None, as rdflib returns if your selection variable
    doesn't appear in your query, are omitted. For example, running this:

       SELECT ?x ?unused { ?x :y :z}

    will return bindings for ?x only, just as if you never included
    ?unused. The sparql engine should probably error on that case, in
    which case this handler will stop getting used.

    But note that if there was an OPTIONAL { ?unused ... } in the
    query, then there's no error but some rows won't get a
    <binding>. See _addOptionalVars in remotegraph.py.

    This is the inverse of parseSparqlResults.
    """
    # this is redundant with a version in rdflib already, although
    # mine uses a default xml namespace that saves quite a few bytes

    # in one test, getQuery spends 27% in queryd and 71% in this function!
    
    return '<?xml version="1.0"?>\n' + flat.flatten(
        Tag("sparql")(xmlns=RESULTS_NS)[
          Tag("head")[
            Tag("variable")(name="notimplemented")
            ],
          Tag("results")(ordered="notimplemented", distinct="notimplemented")[
            (Tag("result")[
              (Tag("binding")(name=k)[term(v)] for k,v in row.items()
               if v is not None)
              ] for row in resultRows)
            ]
          ]
        )

def xmlCountResults(count):
    """a made-up format for count query results"""
    return '<?xml version="1.0"?>\n' + flat.flatten(
        Tag("sparql")(xmlns=RESULTS_NS, **{'xmlns:ext':EXTENDED_NS})[
          Tag("results")[
            Tag("ext:count")[count],
        ]])

def parseCountTree(tree):
    """attempt to get the extended count result from this parsed
    elementtree, or raise ValueError if it's not a count result"""
    results = tree.find(RESULTS_NS_ET + 'results')
    children = list(results)
    if len(children) == 1 and children[0].tag == '{%s}count' % EXTENDED_NS:
        return int(children[0].text)
    raise ValueError("sparql results are not in the extended count format")

def parseSparqlResults(xmlResults):
    """
    list of rows of {var1 : value1, var2 : value2, ...} dicts for the
    given sparql result xml

    this parser is -really- loose.

    pass the ElementTree instead of the string if you want

    This is the inverse of xmlResults."""
    if isinstance(xmlResults, basestring):
        if isinstance(xmlResults, unicode):
            xmlResults = xmlResults.encode('utf-8')
        try:
            tree = ElementTree.fromstring(xmlResults)
        except Exception, e:
            try:
                raise e.__class__("error parsing %r: %s" % (xmlResults, e))
            except:
                raise e
    else:
        tree = xmlResults

    results = tree.find(RESULTS_NS_ET + 'results')
    
    ret = []
    for result in results:
        r = {}
        for binding in result:
            r[binding.get('name')] = parseTerm(binding[0])
        ret.append(r)
    return ret

def test():
    result = [dict(x=URIRef("http://some/uri"),
                   y=Literal("lit1"),
                   z=Literal("dt1", datatype=URIRef("http://some#datatype")),
                   ),
              dict(x=URIRef("http://some/uri2"),
                   y=Literal("lit2")),
              
              ]
    # a streaming xml renderer would probably be better
    print xmlResults(result)
    
    assert parseSparqlResults(xmlResults(result)) == result
    
if __name__ == '__main__':
    test()
