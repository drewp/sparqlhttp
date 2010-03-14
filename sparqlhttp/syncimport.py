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
"""
from __future__ import division
import os, time, logging, traceback
from rdflib import Namespace, Literal
from rdflib.Graph import Graph
from rdflib.syntax.parsers.ntriples import ParseError
from twisted.internet import task
from sparqlhttp.dictquery import contextFromFilename
from xml.utils import iso8601
import xml.sax._exceptions
IMP = Namespace("http://projects.bigasterisk.com/2006/01/syncImport/")
XS = Namespace("http://www.w3.org/2001/XMLSchema#")

log = logging.getLogger('sync')

class SyncImport(object):
    """this object stores a little bit of its own data in each file's context"""
    def __init__(self, graph, inputDirectory=".",
                 contextPrefix="http://example.org",
                 pollSeconds=2):
        """graph is a Graph2 (takes multiple stmts for add())"""
        self.__dict__.update(vars())

        self.lastError = {} # filename : error
        
        self.pollLoop = task.LoopingCall(self.poll)
        self.pollLoop.start(pollSeconds)

    def __del__(self):
        self.pollLoop.stop()

    def poll(self):
        missingFilenames = self.allImportedFilenames()

        for filename in self.allInputFiles():
            if os.path.exists(filename):
                missingFilenames.discard(filename)
            if self.fileIsUpdated(filename):
                log.info("need reload of %s", filename)
                self.reloadContext(filename)

        for filename in missingFilenames:
            # there's a bug where this was making ValueError since the
            # filename wasn't underneath the input directory. The bug
            # is that the polling stops on the first error, requiring
            # a db restart.
            ctx = contextFromFilename(filename, self.contextPrefix,
                                      self.inputDirectory)
            log.info("input file %s disappeared, clearing %s" % (filename, ctx))
            self.graph.remove((None, None, None), context=ctx)
            self.removeImportRecord(ctx)

    def logFileError(self, filename, logFunc, msg):
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
            mtime = os.path.getmtime(filename)
            try:
                ctx = contextFromFilename(filename, self.contextPrefix,
                                          self.inputDirectory)
            except ValueError, e:
                self.logFileError(filename, log.debug,
                      "filename %s doesn't tell us a context- "
                      "skipping (%s)" % (filename, e))

                return False
            if self.lastImportTimeSecs(ctx) < mtime:
                log.debug("%s < %s, file %s is updated" %
                          (self.lastImportTimeSecs(ctx), mtime, filename))
                return True
        except KeyboardInterrupt: raise
        except Exception, e:
            self.logFileError(filename, log.warn,
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
            self.setLastImportTime(ctx, time.time(), filename)
        except (xml.sax._exceptions.SAXParseException,
                ParseError), e:
            log.warn("parse error reading file. (%s)", e)
        except KeyboardInterrupt: raise
        except Exception, e:
            log.error("while trying to reload:\n%s", traceback.format_exc())
        
    def lastImportTimeSecs(self, context):
        """get the import time for a context, in unix seconds; or None
        if it was never imported"""
        importTime = self.graph.value(context, IMP['lastImportTime'])
        if importTime is None:
            log.debug("no imp:lastImportTime for %s" % context)
            return None
        return iso8601.parse(str(importTime))
        
    def allImportedFilenames(self):
        rows = self.graph.queryd(
            "SELECT ?f WHERE { ?ctx <%s> ?f }" % IMP['filename'])
        return set([row['f'] for row in rows])

    def setLastImportTime(self, context, secs, filename):
        """remember the import time for a context"""

        self.removeImportRecord(context)
        self.graph.add((context, IMP['lastImportTime'],
                        Literal(iso8601.tostring(secs, time.altzone),
                                datatype=XS["dateTime"])),
                       (context, IMP['filename'], Literal(filename)),
                       context=IMP['dbInternal#context'])

    def removeImportRecord(self, context):
        log.debug("dropping metadata for context %s" % context)
        self.graph.remove((context, IMP['lastImportTime'], None),
                          (context, IMP['filename'], None),
                          context=IMP['dbInternal#context'])
        
