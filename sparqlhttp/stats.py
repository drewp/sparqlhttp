from __future__ import division
import os, textwrap, time
from nevow import tags as T, flat, inevow
from twisted.python.components import Adapter
from zope.interface import implements

# change site to nevowsite, and the old/new resourcs will work

class Stats(object):
    """gather and report stats about the queries that are made, their
    errors, runtimes, etc"""
    def __init__(self):
        self.powerOnTime = time.time()
        self.queries = 0
        self.lastErrorQuery = ""
        self.lastError = ""
        self.lastQuery = ''

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

        upHours = (time.time() - self.powerOnTime) / 3600
        oneLine = (self.lastQuery or '').replace('\n',' ')
        
        return str(flat.flatten(T.html[
            T.head[T.title[title],
                   T.style(type="text/css")['''
                       span, div {
                         font-family: sans-serif;
                       }
                       div.one-line-query {
                         border: 1px solid #ccc;
                         padding: .2em;
                         background: #f8f8ff;
                       }
                       h2 {
                         color: #6e5636;
                         margin-left: -.5em;
                       }

                       table, td, th {
                          border-top: 1px solid #ddd;
                          border-collapse: collapse;
                        }
                        th, table {
                          border-top: 0px;
                        }
                        th {
                          font-size: 80%;
                          padding-left: .5em;
                          padding-right: .5em;
                        }
                        div.section {
                          padding: 3px;
                          margin-bottom: 6px;
                  
                          margin-left: 1em;
                        }
                        span, th {
                          color:#A2927E;
                          font-style:italic;
                        }
                        '''],
                   ],
            T.body[
              T.h1[title],

              T.div(class_="section")[T.h2["Stats"],
                                      T.div[T.span["Up for "],
                                            "%.1f" % upHours, T.span[" hours"]],
                                      T.div[self.queries, T.span[" queries"]]],
              T.div(class_="section")[T.h2["Last error"],
                                      T.div[T.span["query: "],
                                            T.pre[self.lastErrorQuery]],
                                      T.div[T.span["error: "],
                                            T.pre[self.lastError]]],
              T.div(class_="section")[T.h2["Last query"],
                                      T.pre[reindent(self.lastQuery)],
                                      T.span["Again, as one line:"],
                                      T.div(class_="one-line-query")[oneLine],
                     ],
                
              T.div(class_="section")[T.h2["All query types, by total time spent"],
                                      profStan],
              
              ]
            ]))
        

def stripPrefixes(q):
    """remove PREFIX lines from a query"""
    new = ""
    for line in q.splitlines():
        if not line.strip().startswith("PREFIX"):
            new += line + "\n"
    return new

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
        if fname is None: # client might not be sending filenames
            return ''
        return os.path.basename(fname)

    def rend(self, data):
        def row(k,v):
            query, n, elapsed, rowCount = v

#            if k[0].startswith("(count)"):
#                rowCount = "(count) %s" % rowCount
            return (elapsed, T.tr[T.td["%.2f (%.4f)" % (elapsed, elapsed / n)],
                                  T.td[n],
                                  T.td[T.pre[reindent(stripPrefixes(query))]],
                                  T.td[repr(k[1]) or ''],
                                  T.td["%.1f" % (rowCount / n)],
                                  T.td[[T.div[self.shortFilename(s)] for s in
                                        self.original.sources.get(k,'')]],
                                  ])

        rows = [row(k,v) for k,v in self.original.counts.items()]
        rows.sort(reverse=True)
        return T.table(class_="queryProfile")[
            T.tr[T.th["total secs (per)"],
                 T.th["count"],
                 T.th["query (with sample initBindings; prefixes hidden)"],
                 T.th["bound"],
                 T.th["avg rows"],
                 T.th["sources"]],
            [r[1] for r in rows]]
    
#registerAdapter(QueryProfileView, QueryProfile, inevow.IRenderer)
#queryProfile = QueryProfile()

    
