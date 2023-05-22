import math, json, subprocess, sys, os

def getNativePBSResources():
    def walltime_string_to_seconds(string):
        h, m, s = string.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

    def memory_string_to_mb(string):
        if len(string) < 3: return 1
        userMemSuffix = string[-2:]
        userMemAmount = string[:-2]
        memSuffix = ['b','kb','mb','gb','tb']
        return math.ceil(int(userMemAmount) * pow(1024, (memSuffix.index(userMemSuffix))-2))
    ret = {}

    qstatCmd = '/opt/pbs/bin/qstat'
    if os.path.exists(qstatCmd):
        p = subprocess.Popen([qstatCmd, '-x', '-f', '-F', 'json'], stdout=subprocess.PIPE, stderr=sys.stderr)
        qstatOutput, _ = p.communicate()
        
        if not p.returncode:
            qstatObj = json.loads(qstatOutput)
            for k, v in qstatObj['Jobs'].items():
                if "resources_used" in v:
                    if {"walltime", "mem"} <= set(v["resources_used"]):
                        ret[k] = {
                            'wall-time': walltime_string_to_seconds(v["resources_used"]["walltime"]),
                            'memory': memory_string_to_mb(v["resources_used"]["mem"]),
                        }
    return ret