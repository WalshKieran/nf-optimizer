## nf-optimizer
Simple proof of concept package to generate Nextflow config files with constrained resources based on previous runs. Requires Nextflow for directory inputs.

> **Note**
> This is a concept/test package only - it has only been trialled on a few workflows using **accurate** metrics from PBS Pro.

## Getting Started
### Installation:
```bash
pip install git+https://github.com/WalshKieran/nf-optimizer
```

### Minimal Example:
```bash
nf-optimizer -o resources.config .
nextflow run ... -c resources.config
```

### Typical Example:
```bash
# Allows resuming with updated resources.config
# You must export this for initial AND resumed runs, and should
# not modify other task directives like ext.args when resuming 
# unless you delete associated work directories
export NXF_ENABLE_CACHE_INVALIDATION_ON_TASK_DIRECTIVE_CHANGE=false

# Clamp resources for your target machine/HPC, combine multiple folders
nf-optimizer -m 500 8000 -t 60 3600 -o resources.config /all/runs/* /another/run

# Will lead to a lot of retries if previous runs are not representative
nextflow run ... -c resources.config -resume
```

Note hidden ".nf_optimizer_cache.json" files are created in each supplied directory so that Nextflow metrics can be enhanced with more accurate (but time-limited) HPC stats. Any folder with this file can be reloaded in future.

## Usage
```
usage: nf-optimizer [-h] [-m MEMORY MEMORY] [-t WALLTIME WALLTIME] [-c CONFIDENCE] [--multiplier MULTIPLIER] [--skip_duration SKIP_DURATION]
                    [--output OUTPUT] [--dry-run] [--clean]
                    paths [paths ...]

Simple proof of concept package to generate Nextflow config files with constrained resources based on previous runs.

positional arguments:
  paths                 List of Nextflow project directories or execution traces (.txt). Use traces only if project folders are in use. For projects, all contained
                        runs will be included.

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
                        Skip any (inaccurate) tasks below this duration in seconds. Ignored (-1) for pbspro. (default: 10)
  --output OUTPUT, -o OUTPUT
                        Output file path (default: resources.config)
  --dry-run             Display optimized config instead of writing. (default: False)
  --clean               Delete cached resources and exit. (default: False)
```
## Acknowledgments
* Implemented at UNSW, Sydney