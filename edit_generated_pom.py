#!/usr/bin/env python

'''
Reads the automatically generated Hadoop pom file, removes the "optional"
flag from dependencies so that they could be included transitively into other
projects such as HBase, and removes certain dependencies that are not required
and could even break the code (e.g. an old version of xerces). Writes the
modified project object model XML to standard output.
'''

import os
import re
import sys

from xml.dom.minidom import parse

NON_TRANSITIVE_DEPS = [
    # Old version, breaks HBase
    'xerces',

    # Not used in production
    'checkstyle',
    'jdiff',

    # A release audit tool, probably not used in prod
    'rat-lib',
]

POM_FILE = 'build/ivy/maven/generated.pom'
doc = parse(POM_FILE)
deps = doc.getElementsByTagName('dependencies')[0]

for dep in deps.getElementsByTagName('dependency'):
    for c in dep.childNodes:
        if (c.nodeName == 'artifactId' and
            c.firstChild and
            c.firstChild.nodeValue and
            c.firstChild.nodeValue.strip() in NON_TRANSITIVE_DEPS):
            deps.removeChild(dep)
            break

    for o in dep.getElementsByTagName('optional'):
        dep.removeChild(o)

out_lines = doc.toprettyxml(indent=' ' * 2)
lines = []
for l in out_lines.split('\n'):
    l = l.rstrip()
    if l:
        lines.append(l)
output = '\n'.join(lines)

# Make sure values stay on the same line: <element>value</element>
output = re.sub(
    r'(<([a-zA-Z]+)>)'
    r'\s*([^<>]+?)\s*'
    r'(</\2>)', r'\1\3\4', output)

print output

