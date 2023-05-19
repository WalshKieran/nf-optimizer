import csv, subprocess, sys, json, os, math

from optimizer import Optimizer, Resources
from native import getNativePBSResources
from utils import nf_memory_to_mb, nf_time_to_seconds, seconds_to_nf_time, mb_to_nf_memory

print(f'To combine resume and optimization, export NXF_ENABLE_CACHE_INVALIDATION_ON_TASK_DIRECTIVE_CHANGE=false before ALL runs', file=sys.stderr)

natives = [getNativePBSResources()]
confidence = 0.95
multiplier = 1.2
nextflow_fields = ['hash', 'native_id', 'peak_rss', 'realtime', 'process', 'tag', 'hash', 'exit']
clamp_resources = {"memory": (500, nf_memory_to_mb('124.GB')), "wall-time": (300, nf_time_to_seconds('48.h'))}

if __name__ == "__main__":
    opt = Optimizer(confidence, multiplier)

    for inputDir in sys.argv[1:]:
        # Find project names
        p = subprocess.Popen(['nextflow', 'log'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=inputDir)
        pOut, pErr = p.communicate() 
        if p.returncode: raise Exception(pErr.decode())
        projectNames = [x.split('\t')[2] for i, x in enumerate(pOut.decode().splitlines()) if i%2]

        print(f'Processing {inputDir} ({",".join(projectNames)})')

        # Determine tasks
        p = subprocess.Popen(['nextflow', 'log', '-f', ','.join(nextflow_fields), *projectNames], stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=inputDir)
        tOut, tErr = p.communicate()
        if p.returncode: raise Exception(tOut.decode())

        # Iterate task resources
        nativeToKey = {}
        resources = {}
        for t in tOut.decode().splitlines():

            # Load nextflow non-empty fields
            vals = dict(zip(nextflow_fields, t.split('\t')))
            for k, v in list(vals.items()): 
                if v == '-': vals.pop(k)

            key = vals['native_id'] + vals['hash']
            nativeToKey[vals['native_id']] = key
            
            resources[key] = {
                'category': vals['process'],
                'subcategory': vals.get('tag', ''),
                'memory': nf_memory_to_mb(vals['peak_rss']) if 'peak_rss' in vals else 0, 
                'wall-time': nf_time_to_seconds(vals['realtime']) if 'realtime' in vals else 0,
                'success': vals.get('exit', None) == '0',
            }

        # Overwrite if native resources available
        for n in natives:
            for nativeId, rObj in n.items():
                key = nativeToKey.get(nativeId, None)
                if key: resources[key] = {**resources[key], **rObj}

        # Overwrite with existing
        cache_f = os.path.join(inputDir, '.optimized_cache.json')
        if os.path.exists(cache_f):
            with open(cache_f) as f:
                resources = {**resources, **json.load(f)}

        # Re-write cache
        with open(cache_f, 'w') as f:
            f.write(json.dumps(resources))

        # Add to optimizer
        for k, v in resources.items():
            opt.add_measurement(v['category'], v.get('subcategory', ''), Resources(v, v['success']))

    # Create estimates and write config
    estimates = list(opt.estimate_max_measurements(clamp_resources))
    if not len(estimates): print(f'No estimates generated')
    else:
        conf = 'process {\n'
        conf += f"\tmaxRetries = 3\n"
        conf += "\terrorStrategy = { task.exitStatus in [140,143,137,104,134,139] ? 'retry' : 'finish' }\n"
        conf += "\n"
        for c, r in estimates:
            conf += "\twithName: '%s' {\n" % c.name
            maxRepeats = -1
            if r.values['memory']: 
                repeats = min(3, math.ceil(clamp_resources['memory'][1] / r.values['memory']))
                maxRepeats = max(maxRepeats, repeats)
                conf += f"\t\tmemory = {{ task.attempt < {repeats} ? task.attempt * {mb_to_nf_memory(math.ceil(r.values['memory']))} : {mb_to_nf_memory(clamp_resources['memory'][1])} }}\n"
            if r.values['wall-time']:
                repeats = min(3, math.ceil(clamp_resources['wall-time'][1] / r.values['wall-time']))
                maxRepeats = max(maxRepeats, repeats)
                conf += f"\t\ttime = {{ task.attempt < {repeats} ? task.attempt * {seconds_to_nf_time(math.ceil(r.values['wall-time']))} : {seconds_to_nf_time(clamp_resources['wall-time'][1])} }}\n"
            if maxRepeats < 3: conf += f"\t\tmaxRetries = {maxRepeats}\n"
            conf += '\t}\n'
        conf += '}\n'

        # Report result
        outFile = "resources.config"
        with open(outFile, 'w') as f: f.write(conf)
        print(f'Successfully wrote {os.path.abspath(outFile)} based on {len(resources)} traces')