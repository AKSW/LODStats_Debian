from cache import LodstatsDebianCache

from log import logger

class DatasetManipulator2000(object):
    def __init__(self):
        self.cache = LodstatsDebianCache()

    def getPackages(self):
        resources = self.cache.loadResourcesFromCache() 
        datasets = dict()
        for num, resource in enumerate(resources):
            if(not hasattr(resource, 'package_name')):
                continue
            if(datasets.get(resource.package_name, False)):
                datasets[resource.package_name].append(resource)
            else:
                datasets[resource.package_name] = [resource]
        return datasets

if __name__ == "__main__":
    pass
