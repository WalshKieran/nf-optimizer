import sys, math
from lifelines import WeibullFitter

class Optimizer:
    def __init__(self, confidence, multiplier):
        self.confidence = confidence
        self.multiplier =  multiplier

        self.categories = {}
        self.subcategories = {}

    def isFullCoverage(self):
        return len(self.categories) and all(c.success_count for c in self.categories.values())
    
    def add_measurement(self, category, subcategory, resources):
        categoryObj = self.categories.setdefault(category, Category(category))
        subcategoryObj = self.subcategories.setdefault(subcategory, Subcategory(subcategory)) if subcategory else None

        categoryObj.add_measurement(resources)
        if subcategoryObj: subcategoryObj.add_measurement(categoryObj, resources)

    def count_measurements(self):
        return sum([x.submitted_count for x in self.categories.values()])

    def estimate_max_measurements(self, clamp_resources):
        for c in self.categories.values():
            r = c.estimate_max_measurement(self.confidence)
            if r is None: continue

            r_observed = c.max_measurement()
            for k in Resources.REQUIRED:
                r.values[k] *= (self.multiplier)

            # Clamp and round to nicer values
            for k in clamp_resources.keys():
                r.values[k] = max(r.values[k], clamp_resources[k][0])
                if r.values[k] > clamp_resources[k][1]:
                    if r_observed.values[k] > clamp_resources[k][1]:
                        print(f'ERROR: Existing and estimated resource {k} exceeded clamp {c.name} {r.values[k]} {r_observed.values[k]}', file=sys.stderr)
                        r.values[k] = None
                        continue
                    print(f'WARN: Estimated resources {k} exceeded clamp {c.name} {r.values[k]} {r_observed.values[k]}', file=sys.stderr)
                    r.values[k] = clamp_resources[k][1]

            # Round up to next minute
            if r.values['wall-time']: 
                r.values['wall-time'] = 60 * math.floor(r.values['wall-time']/60)

            yield (c, r)

class Resources:
    REQUIRED = {"memory", "wall-time"}
    def __init__(self, values, isSuccess=True):
        for k in Resources.REQUIRED:
            if k not in values:
                raise ValueError(f"Must define all required resources ({k} missing)")
        self.values = values
        self.isSuccess = isSuccess

class Category():
    def __init__(self, name):
        self.name = name
        self.submitted_count = 0
        self.success_count = 0
        self.measured_resources = []

    def add_measurement(self, resource):
        self.measured_resources.append((resource))
        self.success_count += (1 if resource.isSuccess else 0)
        self.submitted_count += 1

    def estimate_max_measurement(self, confidence=0.95):
        if self.success_count == 0: return None
        ret = {}
        for k in Resources.REQUIRED:
            ret[k] = self._estimate_max([r.values[k] for r in self.measured_resources], [r.isSuccess for r in self.measured_resources], confidence)
        return Resources(ret)
    
    def max_measurement(self):
        if self.success_count == 0: return None
        ret = {}
        for k in Resources.REQUIRED:
            for r in self.measured_resources:
                if k not in ret or ret[k] < r.values[k]:
                    ret[k] = math.ceil(r.values[k])
        return Resources(ret)

    def _estimate_max(self, data, succeeded, confidence):
        if len(data) == 1: return 2*data[0]
        wf = WeibullFitter(alpha=1-confidence)
        wf.fit([x or 0.0001 for x in data], event_observed=succeeded)
        return math.ceil(wf.percentile(1-confidence))

    def __lt__(self, other):
        return (self.submitted_count, self.success_count) > (other.submitted_count, other.success_count)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

class Subcategory():
    def __init__(self, name):
        self.name = name
        self.all_measured_resources = {}

    def add_measurement(self, category, resource):
        self.all_measured_resources[category] = resource

    def __lt__(self, other):
        time_diff = 0
        for cat in self.all_measured_resources.keys():
            if cat in other.all_measured_resources:
                time_diff += (self.all_measured_resources[cat].values["wall-time"] - other.all_measured_resources[cat].values["wall-time"])
        return time_diff < 0

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name