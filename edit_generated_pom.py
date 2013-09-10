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
import subprocess

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
def get_output(doc):
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
    return output


def branch_to_version_mapping(branch) :
    # Do not update pom file unless its a hdfs RC.
    if branch == 'default' or branch == 'trunk':
        return -1
    return ord(branch[0]) - ord('a') + 1

def modify_pom(pom_file, maven_version):
    doc = parse(pom_file)
    version = doc.getElementsByTagName('version')[0]
    version.firstChild.nodeValue = maven_version

    deps = doc.getElementsByTagName('dependencies')[0]
    dep_version = deps.getElementsByTagName('dependency')[0].getElementsByTagName('version')[0]
    dep_version.firstChild.nodeValue = maven_version
    f1=open(pom_file, 'w+')
    f1.write(get_output(doc))
    f1.close()

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

p = subprocess.Popen(['./getBranchAndVersion.sh'], stdout=subprocess.PIPE)
branch_and_version, err = p.communicate()
splits = branch_and_version.split('-')
branch = splits[0]
branch_to_version = branch_to_version_mapping(branch)
if branch_to_version != -1 :
    maven_version = "%s.%s" % (branch_to_version, splits[1])

    # Update the pom with the version specified.
    version = doc.getElementsByTagName('version')[0]
    version.firstChild.nodeValue = maven_version

    # Update the pom with correct artifactId specified.
    artifactId = doc.getElementsByTagName('artifactId')[0]
    artifactId.firstChild.nodeValue = "hadoop-hdfs"

    # Add the branch name to the pom
    root = doc.documentElement
    firstChild = root.childNodes[0]
    name = doc.createElementNS(None,'name')
    txt = doc.createTextNode(branch)
    name.appendChild(txt)
    root.insertBefore(name, firstChild)
    modify_pom('highavailability.pom', maven_version)
    modify_pom('raid.pom', maven_version)
    modify_pom('seekablecompression.pom', maven_version)


print get_output(doc)

