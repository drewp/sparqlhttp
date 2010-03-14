from __future__ import division
import os, textwrap
from nevow import tags as T, flat, inevow
from twisted.python.components import Adapter
from zope.interface import implements

class Stats(object):
    """gather and report stats about the queries that are made, their
    errors, runtimes, etc"""
    def __init__(self):
        self.queries = 0
        self.lastErrorQuery = ""
        self.lastError = ""
        self.lastQuery = None

        self.counts = {} # (queryKey, bound) : (query, n, total elapsed, total rows)
        self.sources = {} # query : [sources]
    
    def ran(self, source, query, queryKey, bound, elapsed, rows):
        bound = tuple(bound)
        key = (queryKey, bound)
        prev = self.counts.get(key, ("", 0, 0, 0))
        self.counts[key] = (query,
                            prev[1] + 1,
                            prev[2] + elapsed,
                            prev[3] + rows)

        if key not in self.sources:
            self.sources[key] = [source]
        elif source not in self.sources[key]:
            self.sources[key].append(source)

    def __getstate__(self):
        return self.counts, self.sources
    def __setstate__(self, s):
        self.counts, self.sources = s
    def clear(self):
        self.counts.clear()
        self.sources.clear()

    def statusPage(self):
        title = 'sparqlhttp server status'

        prof = QueryProfileView(self)
        prof.original = self
        profStan = prof.rend(None)
        
        return str(flat.flatten(T.html[
            T.head[T.title[title],
                   T.style(type="text/css")['''
                        table, td, th {
                          border: 1px solid black;
                          border-collapse: collapse;
                        }
                        th {
                          font-size: 80%;
                        }
                        li {
                          padding: 3px;
                          margin-bottom: 6px;
                        }
                        span, th {
                          color:#A2927E;
                          font-style:italic;
                        }
                        '''],
                   ],
            T.body[
              T.h1[title],
              T.ul[
                T.li[self.queries, T.span[" queries"]],
                T.li[T.span["Last error:"],
                     T.div[T.span["query: "], T.pre[self.lastErrorQuery]],
                     T.div[T.span["error: "], T.pre[self.lastError]]],
                T.li[T.span["Last query:"],
                     T.pre[reindent(self.lastQuery)],
                     T.span["Again, as one line:"],
                     T.div[self.lastQuery.replace('\n','')],
                     ],
                
                T.li[T.span["All query types, by total time spent"],
                     profStan],
                ]
              ]
            ]))
        
    

def reindent(s, width=80):

    """strip off the most whitespace from all lines except first/last,
    and indent 2 spaces. Then cut lines over {width} chars and indent
    the wraps"""
    if s is None:
        return s
    lines = s.splitlines()
    if len(lines) > 2:
        try:
            lines[-1] = lines[-1].strip()        
            minIndent = min([line.split(' ').index(line.split()[0])
                             for line in lines[1:-1] if line.strip()])
            lines[1:-1] = ["  " + line[minIndent:] for line in lines[1:-1]]
        except IndexError:
            pass
    wrapped = []
    for line in lines:
        wrapped.append(textwrap.fill(line, width, subsequent_indent="    ",
                                     break_long_words=False))
        
    return "\n".join(wrapped)

class QueryProfileView(Adapter):
    implements(inevow.IRenderer)

    def shortFilename(self, fname):
        return os.path.basename(fname)

    def rend(self, data):
        def row(k,v):
            query, n, elapsed, rowCount = v

#            if k[0].startswith("(count)"):
#                rowCount = "(count) %s" % rowCount
            return (elapsed, T.tr[T.td["%.2f (%.4f)" % (elapsed, elapsed / n)],
                                  T.td[n],
                                  T.td[T.pre[reindent(query)]],
                                  T.td[repr(k[1])],
                                  T.td["%.1f" % (rowCount / n)],
                                  T.td[[T.div[self.shortFilename(s)] for s in
                                        self.original.sources.get(k,'')]],
                                  ])

        rows = [row(k,v) for k,v in self.original.counts.items()]
        rows.sort(reverse=True)
        return T.table(class_="queryProfile")[
            T.tr[T.th["total secs (per)"],
                 T.th["count"],
                 T.th["query (with sample initBindings)"],
                 T.th["bound"],
                 T.th["avg rows"],
                 T.th["sources"]],
            [r[1] for r in rows]]
    
#registerAdapter(QueryProfileView, QueryProfile, inevow.IRenderer)
#queryProfile = QueryProfile()

    
