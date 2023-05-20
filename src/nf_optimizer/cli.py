import csv, subprocess, json, os, math, argparse, sys

from nf_optimizer.optimizer import Optimizer, Resources
from nf_optimizer.native import getNativePBSResources
from nf_optimizer.utils import nf_memory_to_mb, nf_time_to_seconds, seconds_to_nf_time, mb_to_nf_memory

def main():
    nextflow_fields = ['hash', 'native_id', 'peak_rss', 'realtime', 'process', 'tag', 'hash', 'status']
    natives = {"pbspro": getNativePBSResources}

    # Parse arguments
    parser = argparse.ArgumentParser(
                    prog='nf-optimizer',
                    description=f'Simple proof of concept package to generate Nextflow config files with constrained resources based on existing runs. Read http://github.com/WalshKieran/nf-optimizer carefully before using alongside -resume.',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-m', '--memory', type=int, nargs=2, help="Memory range in megabytes.", default=[500, nf_memory_to_mb('124.GB')])
    parser.add_argument('-t', '--walltime', type=int, nargs=2, help="Walltime range in seconds.", default=[300, nf_time_to_seconds('48.h')])
    parser.add_argument('-c', '--confidence', type=float, help="Confidence of estimates from between 0,1.", default=0.95)
    parser.add_argument('--multiplier', type=float, help="Multiplier for biological/hardware variance.", default=1.2)
    parser.add_argument('--skip_duration', type=int, nargs=1, help=f"Skip any (inaccurate) tasks below this duration. Ignored for {', '.join(natives.keys())}.", default=10)
    parser.add_argument('--output', '-o', help='Output file path', default='resources.config')
    parser.add_argument('--dry-run', help='Display optimized config instead of writing.', action='store_true')
    parser.add_argument('--clean', help='Delete cached resources and exit.', action='store_true')
    parser.add_argument('directories', nargs='+', help="List of Nextflow project directories. All contained runs will be included.") 
    args = parser.parse_args()

    opt = Optimizer(args.confidence, args.multiplier)
    clamp = {"memory": args.memory, "wall-time": args.walltime}
    nativeResources = {} if args.clean else dict(pair for f in natives.values() for pair in f().items())

    count = 0
    for inputDir in [os.path.abspath(x) for x in args.directories]: 
        cache_f = os.path.join(inputDir, '.optimized_cache.json')

        # Delete cache and skip if requested
        if args.clean:
            if os.path.exists(cache_f): os.remove(cache_f)
            continue

        # Load cached resources
        resources = {}
        if os.path.exists(cache_f):
            with open(cache_f) as f:
                resources = {**resources, **json.load(f)}

        # Find project resources directly from Nextflow
        nativeToKey = {}
        p = subprocess.Popen(['nextflow', 'log'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=inputDir)
        pOut, _ = p.communicate() 
        if p.returncode: 
            print(f'Error finding Nextflow runs in {inputDir}:\n{pOut.decode()}', file=sys.stderr)
        else:
            projectNames = [x.split('\t')[2].strip() for i, x in enumerate(pOut.decode().splitlines()) if i>0 and x]
            print(f'Found projects in {inputDir}: {",".join(projectNames)}')

            # Determine tasks
            p = subprocess.Popen(['nextflow', 'log', '-f', ','.join(nextflow_fields), *projectNames], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=inputDir)
            tOut, _ = p.communicate()
            if p.returncode: 
                print(f'Error loading metrics in {inputDir}:\n{tOut.decode()}', file=sys.stderr)
            else:
                # Iterate task resources
                for t in tOut.decode().splitlines():

                    # Load nextflow non-empty fields
                    vals = dict(zip(nextflow_fields, t.split('\t')))
                    for k, v in list(vals.items()): 
                        if v == '-': vals.pop(k)

                    key = vals['native_id'] + vals['hash']
                    nativeToKey[vals['native_id']] = key

                    if resources.get(key, {}).get('native', False): continue
                    resources[key] = {
                        'category': vals['process'],
                        'subcategory': vals.get('tag', ''),
                        'memory': nf_memory_to_mb(vals['peak_rss']) if 'peak_rss' in vals else 0, 
                        'wall-time': nf_time_to_seconds(vals['realtime']) if 'realtime' in vals else 0,
                        'success': vals['status'] in {'COMPLETED', 'CACHED'},
                    }

        # Overwrite new/non-cached with native if possible
        for nativeId, rObj in nativeResources.items():
            key = nativeToKey.get(nativeId, None)
            if key: resources[key] = {**resources[key], **rObj, 'native': True}

        # Skip non-native short jobs
        for k, v in list(resources.items()):
            if not v.get('native', False) and v['wall-time'] <= args.skip_duration: 
                resources.pop(k)

        # Re-write cache
        with open(cache_f, 'w') as f:
            f.write(json.dumps(resources))

        # Add to optimizer
        for k, v in resources.items():
            opt.add_measurement(v['category'], v.get('subcategory', ''), Resources(v, v['success']))

        count += len(resources)

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
        if args.dry_run: 
            print(conf)
        else: 
            with open(args.output, 'w') as f: f.write(conf)
        print(f'Resources successfully esitimated from {count} tasks')
            