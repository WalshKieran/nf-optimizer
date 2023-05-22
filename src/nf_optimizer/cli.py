import csv, subprocess, json, os, math, argparse, sys, itertools

from nf_optimizer.optimizer import Optimizer, Resources
from nf_optimizer.native import getNativePBSResources
from nf_optimizer.utils import nf_memory_to_mb, nf_time_to_seconds, seconds_to_nf_time, mb_to_nf_memory

nextflow_fields = {'hash', 'native_id', 'peak_rss', 'realtime', 'process', 'tag', 'status'}

def iter_project(inputDir):
    # Find project resources directly from Nextflow
    p = subprocess.Popen(['nextflow', 'log'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=inputDir)
    pOut, _ = p.communicate() 
    if p.returncode: 
        print(f'Error finding Nextflow runs in {inputDir}:\n{pOut.decode()}', file=sys.stderr)
    else:
        projectNames = [x.split('\t')[2].strip() for i, x in enumerate(pOut.decode().splitlines()) if i>0 and x]
        print(f'Found projects in {inputDir}: {",".join(projectNames)}')

        # Determine tasks
        p = subprocess.Popen(['nextflow', 'log', '-f', ','.join(list(nextflow_fields)), *projectNames], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=inputDir)
        tOut, _ = p.communicate()
        if p.returncode: 
            print(f'Error loading metrics in {inputDir}:\n{tOut.decode()}', file=sys.stderr)
        else:
            # Iterate task resources
            for t in tOut.decode().splitlines():
                yield dict(zip(nextflow_fields, t.split('\t')))
        
def iter_trace(inputTrace):
    with open(inputTrace, 'r') as f:
        for row in csv.DictReader(f, delimiter='\t'):
            vals = {k: row[k] for k in row.keys() if k in nextflow_fields}

            # By default, process and tag are not included but we can derive them
            if 'name' in row:
                words = row['name'].split()
                vals['process'] = words[0]
                vals['tag'] = words[1][1:-1] if len(words) > 1 and len(words[1]) > 2 else ''

            # Warn if missing required fields
            if len(vals) != len(nextflow_fields):
                print(f'Trace {inputTrace} missing fields: {nextflow_fields - set(vals.keys())}', file=sys.stderr)
                return
            yield vals

def get_cache_path(inputDir):
    return os.path.join(inputDir, '.nf_optimizer_cache.json')

def main():
    natives = {"pbspro": getNativePBSResources}

    parser = argparse.ArgumentParser(
                    prog='nf-optimizer',
                    description=f'Simple proof of concept package to generate Nextflow config files with constrained resources based on previous runs. Read https://github.com/WalshKieran/nf-optimizer carefully before using alongside -resume.',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-m', '--memory', type=int, nargs=2, help="Memory range in megabytes.", default=[500, nf_memory_to_mb('124.GB')])
    parser.add_argument('-t', '--walltime', type=int, nargs=2, help="Walltime range in seconds.", default=[300, nf_time_to_seconds('48.h')])
    parser.add_argument('-c', '--confidence', type=float, help="Confidence of estimates from between 0,1.", default=0.95)
    parser.add_argument('--multiplier', type=float, help="Multiplier for biological/hardware variance.", default=1.2)
    parser.add_argument('--skip_duration', type=int, help=f"Skip any (inaccurate) tasks below this duration in seconds. Ignored (-1) for {', '.join(natives.keys())}.", default=10)
    parser.add_argument('--output', '-o', help='Output file path', default='resources.config')
    parser.add_argument('--dry-run', help='Display optimized config instead of writing.', action='store_true')
    parser.add_argument('--clean', help='Delete cached resources and exit.', action='store_true')
    parser.add_argument('paths', nargs='+', help="List of Nextflow project directories or execution traces (.txt). Use traces only if project folders are in use. For projects, all contained runs will be included.") 
    args = parser.parse_args()

    opt = Optimizer(args.confidence, args.multiplier)
    clamp = {"memory": args.memory, "wall-time": args.walltime}
    nativeResources = {} if args.clean else dict(pair for f in natives.values() for pair in f().items())

    # Determine which inputs are traces and whether we can skip running Nextflow
    traces = {}
    shouldRunNF = {}
    inputDirs = set()
    for x in args.paths:
        full = os.path.abspath(x)
        if full.endswith('.txt'): 
            parent = os.path.dirname(full)
            traces.setdefault(parent,[]).append(full)
            inputDirs.add(parent)
        else: 
            shouldRunNF[full] = True
            inputDirs.add(full)

    resources = {}
    keysToCache = {}
    nativeToKey = {}
    for inputDir in inputDirs: 
        currResources = {}
        cache_f = get_cache_path(inputDir)

        # Delete cache and skip if requested
        if args.clean:
            if os.path.exists(cache_f): os.remove(cache_f)
            continue

        # Load cached resources, ignore errors
        if os.path.exists(cache_f):
            try:
                with open(cache_f) as f: currResources = json.load(f)
            except: pass

        projectIterable = iter_project(inputDir) if inputDir in shouldRunNF else []
        traceIterable = itertools.chain(*[iter_trace(t) for t in traces[inputDir]]) if inputDir in traces else []

        for vals in itertools.chain(projectIterable, traceIterable):
            # Remove n/a fields
            for k, v in list(vals.items()): 
                if v == '-': vals.pop(k)

            key = vals['native_id'] + vals['hash']
            nativeToKey[vals['native_id']] = key

            # Add, only overwrite if native
            if currResources.get(key, {}).get('native', False): continue
            currResources[key] = {
                'category': vals['process'],
                'subcategory': vals.get('tag', ''),
                'memory': nf_memory_to_mb(vals['peak_rss']) if 'peak_rss' in vals else 0, 
                'wall-time': nf_time_to_seconds(vals['realtime']) if 'realtime' in vals else 0,
                'success': vals['status'] in {'COMPLETED', 'CACHED'},
            }

        # Save intention to cache so that natives from other caches may contribute
        keysToCache[inputDir] = currResources.keys()

        # Add to global resources, overwriting any non-natives
        for k, v in currResources.items():
            if k not in resources or (v.get('native', False) and not resources[k].get('native', False)):
                resources[k] = v

    # Overwrite new/non-cached with native if possible
    for nativeId, rObj in nativeResources.items():
        key = nativeToKey.get(nativeId, None)
        if key: currResources[key] = {**currResources[key], **rObj, 'native': True}
    
    # Re-write caches
    if not args.clean:
        for inputDir, keys in keysToCache.items():
            with open(get_cache_path(inputDir), 'w') as f:
                f.write(json.dumps({k: resources[k] for k in keys}))
        print(f'Caches successfully written inside {len(inputDirs)} input {"path" if len(inputDirs) == 1 else "paths"}')

    # Add to optimizer
    for k, v in resources.items():
        if v.get('native', False) or v['wall-time'] >= args.skip_duration:
            opt.add_measurement(v['category'], v.get('subcategory', ''), Resources(v, v['success']))

    # Create estimates and write config
    estimates = list(opt.estimate_max_measurements(clamp))
    if not len(estimates): print(f'No estimates generated', file=sys.stderr)
    else:
        conf = 'process {\n'
        conf += f"\tmaxRetries = 3\n"
        conf += "\terrorStrategy = { task.exitStatus in [140,143,137,104,134,139] ? 'retry' : 'finish' }\n"
        conf += "\n"
        for c, r in estimates:
            conf += "\twithName: '%s' {\n" % c.name
            maxRepeats = -1
            if r.values['memory']: 
                repeats = min(3, math.ceil(clamp['memory'][1] / r.values['memory']))
                maxRepeats = max(maxRepeats, repeats)
                conf += f"\t\tmemory = {{ task.attempt < {repeats} ? task.attempt * {mb_to_nf_memory(math.ceil(r.values['memory']))} : {mb_to_nf_memory(clamp['memory'][1])} }}\n"
            if r.values['wall-time']:
                repeats = min(3, math.ceil(clamp['wall-time'][1] / r.values['wall-time']))
                maxRepeats = max(maxRepeats, repeats)
                conf += f"\t\ttime = {{ task.attempt < {repeats} ? task.attempt * {seconds_to_nf_time(math.ceil(r.values['wall-time']))} : {seconds_to_nf_time(clamp['wall-time'][1])} }}\n"
            if maxRepeats < 3: conf += f"\t\tmaxRetries = {maxRepeats}\n"
            conf += '\t}\n'
        conf += '}\n'

        # Output result
        if args.dry_run: print(conf)
        else: 
            with open(args.output, 'w') as f: f.write(conf)
        taskCount = opt.count_measurements()
        print(f'Resources successfully estimated from {taskCount} {"task" if taskCount == 1 else "tasks"}')
