#!/usr/bin/env python
import sys, json;

def rule(e):
    if 'rule' in e:
        r = e['rule']
        if "*" in r:
            return r.replace("=", '=g"') + '"'
        else:
            return r.replace("=", '="') + '"'
    else:
        return '_'

for e in json.load(sys.stdin):
    print '{} {{ $class := "{}"; $rate := {}; return; }}'.format(rule(e), e['class'], e['rate'] * 3)
