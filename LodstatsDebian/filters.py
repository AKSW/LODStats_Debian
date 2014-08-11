import re
import copy
from cache import LodstatsDebianCache

class DatasetFiltering(object):

    def __init__(self, datasets):
        self.datasets = datasets
        self.cache = LodstatsDebianCache()

    def applyFilters(self):
        for method in dir(self):
            if(method.startswith('filter')):
                exec("self.datasets = self.%s(self.datasets)"%(method,))
        return self.datasets

    def filterDuplicateResources(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        for dataset in datasets_copy:
            if(len(datasets_copy[dataset]) > 1):
                for num_fixed, res_fixed in enumerate(datasets_copy[dataset]):
                    if(res_fixed == {}):
                        continue
                    for num_iter, res_iter in enumerate(datasets_copy[dataset]):
                        if(res_iter == {}):
                            continue
                        if(num_fixed != num_iter and res_fixed.url == res_iter.url):
                            datasets_copy[dataset][num_iter] = {}

        #wipe empty resources
        datasets_wiped = self._purgeResources(datasets_copy)
        datasets_purged = self._purgeDatasets(datasets_wiped)

        return datasets_purged

    def filterMoreThanOneDump(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        list_to_delete = []
        for dataset in datasets_copy:
            if(len(datasets_copy[dataset]) > 1):
                for num_fixed, res_fixed in enumerate(datasets_copy[dataset]):
                    if(res_fixed != {}):
                        for num_iter, res_iter in enumerate(datasets_copy[dataset]):
                            if(res_iter != {}):
                                if(num_fixed != num_iter and res_fixed.format == res_iter.format):
                                    if(dataset not in list_to_delete):
                                        list_to_delete.append(dataset)

        for dataset in list_to_delete:
            del(datasets_copy[dataset])

        return datasets_copy

    def filterMoreThanMillionTriples(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        list_to_delete = []
        for dataset in datasets_copy:
            for resource in datasets_copy[dataset]:
                if(resource != {} and resource.triples > 1000000):
                    if(dataset not in list_to_delete):
                        list_to_delete.append(dataset)

        for dataset in list_to_delete:
            del(datasets_copy[dataset])

        return datasets_copy

    def filterDatasetsNoFreeLicenses(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        list_to_delete = []

        for dataset in datasets_copy:
            #Try to load from cache
            package = self.cache.getDataset(dataset)
            if(not package.isopen):
                list_to_delete.append(dataset)

        for dataset in list_to_delete:
            del(datasets_copy[dataset])

        return datasets_copy

    def filterMetaExamplesApis(self, datasets):
        """
            Filtering out archives for now
        """
        datasets_copy = copy.deepcopy(datasets)

        for dataset in datasets_copy:
            for num, resource in enumerate(datasets_copy[dataset]):
                if(re.match( r'api', resource.format, re.M|re.I) or
                   re.match( r'example', resource.format, re.M|re.I) or
                   re.match( r'meta', resource.format, re.M|re.I) or
                   re.match( r'owl', resource.format, re.M|re.I) or
                   re.match( r'ravensburg-local-shopping-graph', dataset, re.M|re.I) or
                   re.match( r'html', resource.format, re.M|re.I) or
                   re.match( r'.*\.gz', resource.url, re.M|re.I) or
                   re.match( r'.*\.tgz', resource.url, re.M|re.I) or
                   re.match( r'.*\.zip', resource.url, re.M|re.I) or
                   re.match( r'http://lov.okfn.org/dataset/lov', resource.url, re.M|re.I) or
                   re.match( r'http://www.ontosearch.com/', resource.url, re.M|re.I)):
                    datasets_copy[dataset][num] = {}

        datasets_wiped = self._purgeResources(datasets_copy)
        return datasets_wiped

    def filterOneResource(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        for dataset in datasets_copy:
            for resource in datasets_copy[dataset]:
                if(re.match(r'ntriple', resource.format)):
                    datasets_copy[dataset] = [resource]
                    break

        for dataset in datasets_copy:
            for resource in datasets_copy[dataset]:
                datasets_copy[dataset] = resource
                break

        return datasets_copy

    def _purgeResources(self, datasets):
        datasets_wiped = {}
        for dataset in datasets:
            for resource in self._ifilter(lambda x: x == {}, datasets[dataset]):
                if(datasets_wiped.get(resource.package_name, False)):
                    datasets_wiped[resource.package_name].append(resource)
                else:
                    datasets_wiped[resource.package_name] = [resource]
        return datasets_wiped

    def _purgeDatasets(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        to_purge = []
        for dataset in datasets_copy:
            if(len(datasets_copy[dataset]) == 0):
                to_purge.append(dataset)

        for dataset in to_purge:
            del(datasets_copy[dataset])

        return datasets_copy

    def _ifilter(self, predicate, iterable):
        if predicate is None:
            predicate = bool
        for x in iterable:
            if not predicate(x):
                yield x

if __name__ == "__main__":
    datasets = []
    df = DatasetFiltering(datasets)
    df.applyFilters()
    print "hi"
