## nf-optimizer
Simple tool to generate Nextflow config files with constrained resources based on previous runs. Depends on Nextflow.

> **Note**
> This package is a concept/test only. It is only tested on a few workflows.

## Getting Started
### Installation:
```bash
pip install git+https://github.com/WalshKieran/nf-optimizer
```

### Minimal Usage:
```bash
nf-optimizer -o resources.config .
nextflow run ... -c resources.config
```

### Typical Usage:
```bash
# Allows resuming with updated resources.config - you must export this for initial AND resumed runs, and should not modify other task directives like ext.args when resuming (unless you delete associated work directories)
export NXF_ENABLE_CACHE_INVALIDATION_ON_TASK_DIRECTIVE_CHANGE=false

# Clamp resources for your target machine/HPC, combine multiple folders
nf-optimizer -m 500 8000 -t 60 3600 -o resources.config /all/runs/* /another/run

# Will lead to a lot of retries if previous runs are not representative
nextflow run ... -c resources.config -resume
```

Note hidden files ".optimized_cache.json" are created in each supplied directory, since Nextflow metrics may be enhanced with more accurate (but time-limited) HPC stats. Any folder with this cache can be reloaded in future.

## Usage
```bash
usage: nf-optimizer [-h] [-m MEMORY MEMORY] [-t WALLTIME WALLTIME] [-c CONFIDENCE] [--multiplier MULTIPLIER] [--skip_duration SKIP_DURATION]
                    [--output OUTPUT] [--dry-run] [--clean]
                    directories [directories ...]

Simple proof of concept package to generate Nextflow config files with constrained resources based on existing runs.

positional arguments:
  directories           List of Nextflow project directories. All contained runs will be included.

options:
  -h, --help            show this help message and exit
  -m MEMORY MEMORY, --memory MEMORY MEMORY
                        Memory range in megabytes. (default: [500, 124000])
  -t WALLTIME WALLTIME, --walltime WALLTIME WALLTIME
                        Walltime range in seconds. (default: [300, 172800])
  -c CONFIDENCE, --confidence CONFIDENCE
                        Confidence of estimates from between 0,1. (default: 0.95)
  --multiplier MULTIPLIER
                        Multiplier for biological/hardware variance. (default: 1.2)
  --skip_duration SKIP_DURATION
                        Skip any (inaccurate) tasks below this duration. Ignored for pbspro. (default: 10)
  --output OUTPUT, -o OUTPUT
                        Output file path (default: resources.config)
  --dry-run             Display optimized config instead of writing. (default: False)
  --clean               Delete cached resources and exit. (default: False)
```
## Acknowledgments
* Implimented at UNSW, Sydney