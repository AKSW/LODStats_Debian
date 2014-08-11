from datasetmanipulation import DatasetManipulator2000
from filters import DatasetFiltering
from ckandebianconfig import CkanDebianConfig
from creator import DebianPackageCreator
from cache import LodstatsDebianCache


class LodstatsDebian(object):
    def __init__(self):
        self.cache = LodstatsDebianCache()
        self.dm2000 = DatasetManipulator2000()
        self.datasets = self.dm2000.getPackages()
        #filters
        self.filtering = DatasetFiltering(self.datasets)
        self.datasets = self.filtering.applyFilters()

        self.datasetConfigs = self.generateConfigs(self.datasets)
        self.creator = DebianPackageCreator()
        self.creator.createDatasets(self.datasets, self.datasetConfigs)

    def generateConfigs(self, datasets):
        configs = {}
        for dataset in datasets:
            package = self.cache.getDataset(dataset)
            config = CkanDebianConfig(package, datasets[dataset])
            configs[dataset] = config
        return configs

if __name__ == "__main__":
    lodstatsdebian = LodstatsDebian()
    #import ipdb; ipdb.set_trace()
