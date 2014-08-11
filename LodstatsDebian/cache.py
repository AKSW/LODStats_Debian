import os
import cPickle
import requests

from postgres import LodstatsDB
from log import logger

class LodstatsDebianCache(object):
    cacheFolder = 'cache/'
    datasetCacheFolder = 'dataset-cache/'
    datasetMetaFolder = 'dataset-meta/'

    def __init__(self):
        #check if cache folder exists and create if necessary
        self.checkAndCreate(self.cacheFolder)
        self.checkAndCreate(self.datasetCacheFolder)
        self.checkAndCreate(self.datasetMetaFolder)
        self.db = LodstatsDB()

    def checkAndCreate(self, folder):
        if(not os.path.exists(folder)):
            os.makedirs(folder)

    def loadResourcesFromCache(self):
        from os import listdir
        from os.path import isfile, join
        onlyfiles = [ f for f in listdir('cache/') if isfile(join('cache/',f)) ]
        resources = []
        for f in onlyfiles:
            st = open('cache/'+f, 'rU')
            res = cPickle.load(st)
            resources.append(res)
        return resources

    def _writeObjToFile(self, obj, filename):
        f = open(os.path.join(self.cacheFolder, filename), 'wb+')
        cPickle.dump(obj, f)
        f.close()

    def updateCache(self):
        lodstatsResources = self.db.getResources()
        dump = []
        length = len(lodstatsResources)
        i = 0
        for (uri, triples, void) in lodstatsResources:
            logger.info("Dumping %s out of %s" % (str(i), str(length)))
            i += 1
            resource = self.getMetadataFromCkan(uri)
            if(resource == []):
                continue
            resource.triples = triples
            resource.void = void
            self._writeObjToFile(resource, "resource"+str(i))
            dump.append(resource)
        return dump

    def getMetadataFromCkan(self, uri):
        from csv2rdf.ckan.resource import Resource
        resource = Resource('')
        try:
            resource = resource.search_by_uri(uri)
            logger.info("Resource %s - SUCCESS" % uri.encode('utf-8'))
        except BaseException as e:
            logger.error("Cannot load resource: %s - %s" % (uri.encode('utf-8'), str(e)))
            resource = []
        return resource

    def getRdfMetadata(self, datasetName):
        rdfMetadata = self._datasetMetaGet(datasetName)
        if(not rdfMetadata):
            url = "http://datahub.io/dataset/"+datasetName+".rdf"
            r = requests.get(url)
            rdfMetadata = r.content
            self._datasetMetaPut(datasetName, rdfMetadata)
        return rdfMetadata

    def _datasetMetaPut(self, datasetName, meta):
        filename = os.path.join(self.datasetMetaFolder, datasetName)
        cPickle.dump(meta, open(filename, 'wb'), -1)

    def _datasetMetaGet(self, datasetName):
        filename = os.path.join(self.datasetMetaFolder, datasetName)
        if(os.path.isfile(filename)):
            return cPickle.load(open(filename, 'rU'))
        else:
            return False

    def _datasetCachePut(self, package):
        filename = os.path.join(self.datasetCacheFolder, package.name)
        cPickle.dump(package, open(filename, 'wb'), -1)

    def _datasetCacheGet(self, package_name):
        filename = os.path.join(self.datasetCacheFolder, package_name)
        return cPickle.load(open(filename, 'rU'))

    def getDataset(self, dataset_name):
        from csv2rdf.ckan.package import Package
        package = {}
        filename = os.path.join(self.datasetCacheFolder, dataset_name)
        if(os.path.isfile(filename)):
            package = self._datasetCacheGet(dataset_name)
        else:
            package = Package(dataset_name)
            del(package.ckan)
            logger.info("Dumping package %s" % dataset_name)
            self._datasetCachePut(package)
        return package

if __name__ == "__main__":
    ldcache = LodstatsDebianCache()
    ldcache.updateCache()
