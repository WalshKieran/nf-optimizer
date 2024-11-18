## Nextflow Optimizer
Simple proof of concept package to generate Nextflow config files with constrained resources based on previous runs. Requires Nextflow for directory inputs.

> [!NOTE]
> This is a concept/test package only - it has only been trialled on a few workflows using **accurate** metrics from PBS Pro.

## Getting Started
### Installation:
```bash
pip install git+https://github.com/WalshKieran/nf-optimizer

# Note on HPC, add python module >= 3.7 and then run the following preferred method:
python -m pip install --user git+https://github.com/WalshKieran/nf-optimizer
```

### Minimal Example:
```bash
nf-optimizer -o resources.config .
nextflow run ... -c resources.config
```

### Typical Example:

1. Limit the samples in your samplesheet (for a better upper bound, you could even select the largest samples):
``` bash
head -n 5 samplesheet.csv > samplesheet_4.csv
```

2. Run Nextflow on limited samples:
``` bash
export NXF_ENABLE_CACHE_INVALIDATION_ON_TASK_DIRECTIVE_CHANGE=false
nextflow run ... --input samplesheet_4.csv
```

3. Generate resources.config (limited to ~120GB, 12 hours):
``` bash
nf-optimizer -m 500 120000 -t 300 43200 -o resources.config --skip_duration -1 .
```

4. Run Nextflow on all samples:
``` bash
export NXF_ENABLE_CACHE_INVALIDATION_ON_TASK_DIRECTIVE_CHANGE=false
nextflow run ... --input samplesheet.csv -c resources.config -resume
```

> [!WARNING]
> The environment variable `NXF_ENABLE_CACHE_INVALIDATION_ON_TASK_DIRECTIVE_CHANGE` is set to `false` to allow resources.config to be updated without breaking -resume.
> Unfortunately, this means other changes to task directives (e.g. ext.args) will NOT trigger task re-execution.
> In this case, you should manually delete the associated work directories before resuming.

Hidden ".nf_optimizer_cache.json" files are created in each supplied directory so that Nextflow metrics can be enhanced with more accurate (but time-limited) HPC stats. Any folder with this file can be reloaded in future.

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