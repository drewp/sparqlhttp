#!/usr/bin/env python
from distutils.core import setup

setup(name="sparqlhttp",
      version="1.4",
      description="HTTP SPARQL server and client for twisted and rdflib",
      author="Drew Perttula",
      author_email="drewp@bigasterisk.com",
      url="http://projects.bigasterisk.com/sparqlhttp/",
      download_url="http://projects.bigasterisk.com/sparqlhttp-1.4.tar.gz",

      packages=['sparqlhttp'],
      py_modules=['test.shared'],
      data_files=[('', ['doc.html'])],
      package_data={'sparqlhttp' : ['query']},
      
      classifiers=[ # http://www.python.org/pypi?:action=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
)



