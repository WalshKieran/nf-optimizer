import math, argparse

def seconds_to_nf_time(s):
    return f"{math.ceil(s/60)}.m"

def mb_to_nf_memory(mb):
    return f"{mb}.MB"

def nf_time_to_seconds(t):
    s = 0
    for v in t.split():
        if v.endswith('ms'): s += float(v[:-2]) / 1000
        elif v.endswith('h'): s += 3600 * float(v[:-1])
        elif v.endswith('m'): s += 60 * float(v[:-1])
        elif v.endswith('s'): s += float(v[:-1])
    return math.ceil(s)

def nf_memory_to_mb(string):
    if string == '0': return 0
    userMemSuffix = string[-2:]
    userMemAmount = string[:-3]
    memSuffix = ['KB','MB','GB','TB']
    return math.ceil((float(userMemAmount) * pow(10, (memSuffix.index(userMemSuffix) * 3)-3)))
    