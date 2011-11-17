#!/usr/bin/env python

from setuptools import setup

setup(name="sparqlhttp",
      version="1.12",
      description="HTTP SPARQL server and client for twisted and rdflib",
      author="Drew Perttula",
      author_email="drewp@bigasterisk.com",
      url="http://projects.bigasterisk.com/sparqlhttp/",
      download_url="http://projects.bigasterisk.com/sparqlhttp/sparqlhttp-1.12.tar.gz",

      # this seems to be making other projects break, especially with buildout
      #setup_requires=['setuptools_trial >= 0.5'],

      packages=['sparqlhttp'],
      data_files=[('', ['doc.html'])],
      package_data={'sparqlhttp' : ['query']},
      test_suite="sparqlhttp.test",

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



