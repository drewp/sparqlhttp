"""
common code for clients that need to make one request and print its output
"""
import sys
from twisted.internet import reactor

def run(deferred, printFunc=lambda result: None):
    def printAndExit(result):
        printFunc(result)
        reactor.stop()

    def failed(err):
        print >>sys.stderr, err.value
        reactor.stop()
        return err
        
    deferred.addCallbacks(printAndExit, failed)
    reactor.run()
    
