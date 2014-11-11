"""
formerly 'MergedFile'

watch a tree of files, arranged the same way Graph2 would save them:

{treeprefix}/{dir}/{dir}/{name}.n3 -> {contextprefix}/{dir}/{dir}/{name}#context

When a file changes, reload it, replacing the previous contents of the
context.

When a file disappears, remove the context.

The file extensions can be n3/nt/rdf. If there are multiple extensions
on the same name, behavior is undefined.

todo:
when scanning a file that was broken, don't relog the exact same error

web interface to examine the dbInternal context

an error in the poll makes it stop running for the rest of the process
"""
from __future__ import division
import os, logging, traceback, re, datetime
from rdflib import Namespace, Literal
try:
    from rdflib.plugins.parsers.ntriples import ParseError
except ImportError:
    # older rdflib
    from rdflib.syntax.parsers.ntriples import ParseError
from twisted.internet import task
from sparqlhttp.dictquery import contextFromFilename
from dateutil.parser import parse
from dateutil.tz import tzlocal

import xml.sax._exceptions
IMP = Namespace("http://projects.bigasterisk.com/2006/01/syncImport/")
XS = Namespace("http://www.w3.org/2001/XMLSchema#")

log = logging.getLogger('sync')

class SyncImport(object):
    """this object stores a little bit of its own data in each file's context"""
    def __init__(self, graph, inputDirectory=".",
                 contextPrefix="http://example.org",
                 pollSeconds=2, polling=True):
        """graph is a Graph2 (takes multiple stmts for add())"""
        self.__dict__.update(vars())

        self.lastError = {} # filename : error

        if polling:
            self.pollLoop = task.LoopingCall(self._poll)
            self.pollLoop.start(pollSeconds)

    def stop(self):
        """stop any polling that might still be running. This is
        mainly for unittests"""
        if self.pollLoop:
            self.pollLoop.stop()
            self.pollLoop = None

    __del__ = stop

    def _poll(self):
        missingFilenames = self.allImportedFilenames()

        for filename in self.allInputFiles():
            # TODO: up here, run contextFromFilename since it would cull out
            # files of the wrong extension.
            
            if os.path.exists(filename): # TODO: this is always true, right? just an extra stat
                missingFilenames.discard(filename)
            if self.fileIsUpdated(filename):
                log.info("need reload of %s", filename)
                self.reloadContext(filename)

        for filename in missingFilenames:
            self.fileDisappeared(filename)
            
    def fileDisappeared(self, filename):
        """
        this file is no longer on disk; drop its graph and bookkeeping triples
        """
        # there's a bug where this was making ValueError since the
        # filename wasn't underneath the input directory. The bug
        # is that the polling stops on the first error, requiring
        # a db restart.
        try:
            ctx = contextFromFilename(filename, self.contextPrefix,
                                      self.inputDirectory)
        except ValueError:
            # if we can't figure out the context, it might be
            # because this filename is handled by a different
            # SyncImport with another inputDirectory, which is
            # cool
            return
        log.info("input file %s disappeared, clearing %s" % (filename, ctx))
        self.graph.subgraphClear(ctx)
        self.removeImportRecord(ctx)
        
    def _logFileError(self, filename, logFunc, msg):
        if msg == self.lastError.get(filename, None):
            return
        self.lastError[filename] = msg
        logFunc(msg)

    def allInputFiles(self):
        """filenames in the input tree

        Names are relative to the inputDirectory."""
        for root, dirs, files in os.walk(self.inputDirectory):
            for filename in files:
                filename = os.path.join(root, filename)
                yield filename
                
    def fileIsUpdated(self, filename):
        """is the file's mtime newer than when we last imported it"""
        try:
            try:
                ctx = contextFromFilename(filename, self.contextPrefix,
                                          self.inputDirectory)
            except ValueError, e:
                self._logFileError(filename, log.debug,
                      "filename %s doesn't tell us a context- "
                      "skipping (%s)" % (filename, e))

                return False
            # we get the mtime of the target file, not a symlink
            mtime = datetime.datetime.fromtimestamp(
                os.path.getmtime(os.path.realpath(filename)), tzlocal())
            last = self.lastImportTime(ctx)
            if not last or last < mtime:
                log.debug("%s < %s, file %s is updated" %
                          (last.isoformat() if last else 'None',
                           mtime.isoformat(), filename))
                return True
        except KeyboardInterrupt: raise
        except Exception, e:
            self._logFileError(filename, log.warn,
                      "error scanning file %s: %s" % (filename, e))
        return False
                    
    def reloadContext(self, filename):
        """update the context to contain the contents of the
        file. There may be a time when the context is partially empty
        or partially full.

        filename is relative to inputDirectory"""
        ctx = contextFromFilename(filename, self.contextPrefix,
                                  self.inputDirectory)
        log.debug("reloading %s", ctx)
        try:
            ext = os.path.splitext(filename)[1].replace('.', '')
            self.graph.safeParse(filename, publicID=ctx, format=ext)
            self.setLastImportTime(ctx, datetime.datetime.now(tzlocal()),
                                   filename)
        except (xml.sax._exceptions.SAXParseException,
                ParseError), e:
            log.warn("parse error reading file. (%s)", e)
        except KeyboardInterrupt: raise
        except Exception, e:
            log.error("while trying to reload:\n%s", traceback.format_exc())

    def reloadContextSesame(self, filename):
        """a version that just uploads the file to sesame for parsing
        and graph replacement. Currently using internal APIs;
        loadFromFile should be made public"""
        ctx = contextFromFilename(filename, self.contextPrefix,
                                  self.inputDirectory)
        log.debug("reloading %s", ctx)
        assert filename.endswith(('.n3', '.nt'))
        n3 = open(filename).read()
        # rdflib makes prefixes like _1; sesame rejects them. This
        # might match unexpected stuff!
        n3 = re.sub(r'_(\d+):', lambda m: 'prefix_%s:' % m.group(1), n3)
        now = datetime.datetime.now(tzlocal())
        try:
            self.graph._request("PUT", path="/statements",
                                queryParams={'context' : ctx.n3()},
                                payload=n3,
                                headers={'Content-Type' : 'text/rdf+n3'})
            self.setLastImportTime(ctx, now, filename)
        except KeyboardInterrupt: raise
        except Exception, e:
            log.error("while trying to reload:\n%s", traceback.format_exc())

        
    reloadContext = reloadContextSesame

    def lastImportTime(self, context):
        """get the import time for a context as datetime; or None
        if it was never imported"""
        if 1:
            # natural version
            importTime = self.graph.value(context, IMP['lastImportTime'])
        else:
            # alternate version for speed
            #
            # self.graph.value might use some optimizations and LRUCache
            # if you have one, but the overhead of dealing with this query
            # was taking like 2ms. If we just use raw rdflib, the query
            # goes 10x faster. Ideally this would be solved by optimizing
            # the Graph2 and caching layers instead.
            innerGraph = self.graph.graph
            importTime = innerGraph.value(context, IMP['lastImportTime'])
        
        if importTime is None:
            log.debug("no imp:lastImportTime for %s" % context)
            return None
        return parse(str(importTime))
        
    def allImportedFilenames(self):
        rows = self.graph.queryd(
            "SELECT ?f WHERE { ?ctx <%s> ?f }" % IMP['filename'])
        return set([row['f'] for row in rows])

    def setLastImportTime(self, context, t, filename):
        """remember the import time for a context"""

        self.removeImportRecord(context)
        self.graph.add([(context, IMP['lastImportTime'],
                         Literal(t.isoformat(), datatype=XS["dateTime"])),
                        (context, IMP['filename'], Literal(filename))],
                       context=IMP['dbInternal#context'])

    def removeImportRecord(self, context):
        log.debug("dropping metadata for context %s" % context)
        self.graph.remove([(context, IMP['lastImportTime'], None),
                           (context, IMP['filename'], None)],
                          context=IMP['dbInternal#context'])
        
