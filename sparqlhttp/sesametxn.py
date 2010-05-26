"""
sesame's transaction xml format, application/x-rdftransaction

Docs are better over here: http://www.allegrograph.net/agraph/support/documentation/3.2/http-protocol.html
But there may be some incorrect details with respect to contexts.
"""
from lxml.etree import Element, tostring
from rdflib import URIRef, Literal

def transactionDoc(ops):
    """
    ops is a list of tuples:
      (operation, subj, pred, obj, ctx)

    operation is either 'add' or 'remove'. Any of the other values may
    be None.
    """

    doc = Element("transaction")
    for op, s, p, o, c in ops:
        if op == 'remove':
            step = Element("remove")
            for node in [s,p,o]:
                step.append(elemFromRdflib(node))
            if c is not None:
                # this is from
                # http://repo.aduna-software.org/websvn/blame.php?repname=aduna&path=/org.openrdf/sesame/branches/2.2/core/http/protocol/src/main/java/org/openrdf/http/protocol/transaction/TransactionSAXParser.java&rev=0&sc=1
                # line 178; context is the 4th child of the <remove> tag 
                step.append(elemFromRdflib(c))
            doc.append(step)
        else:
            raise NotImplementedError

    return tostring(doc)

def elemFromRdflib(node):
    """lxml Element for the given URIRef/Literal"""
    if isinstance(node, URIRef):
        e = Element("uri")
        e.text = node
        return e
    elif isinstance(node, Literal):
        e = Element("literal")
        e.text = node
        if node.datatype is not None:
            e.attrib['datatype'] = node.datatype
        if node.lang is not None:
            e.attrib['xml:lang'] = node.lang
        return e
    else:
        raise NotImplementedError
