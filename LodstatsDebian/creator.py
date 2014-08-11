import os
import requests
import subprocess
import shutil
from cache import LodstatsDebianCache
from log import logger
from rdf2rdf import RDF2RDF
from rdfparser import RdfParser

class DebianPackageCreator(object):
    datasetsFolder = 'datasets'

    def __init__(self):
        self.cache = LodstatsDebianCache()
        pass

    def createDatasets(self, datasets, configs):
        for dataset in datasets:
            logger.info("Creating debian config for %s" % dataset)
            datasetFolder = os.path.join(self.datasetsFolder, dataset)
            if not os.path.exists(datasetFolder):
                os.makedirs(datasetFolder)

            datasetConfig = os.path.join(datasetFolder, configs[dataset].datasetId + ".cfg")
            if os.path.isfile(datasetConfig):
                logger.info("debian config already exists, skipping")
                continue

            ###Download dataset RDF data
            try:
                r = requests.get(datasets[dataset].url, stream=True, timeout=1)
                datasetFilepath = os.path.join(datasetFolder, configs[dataset].datasetId)
                with open(datasetFilepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            f.flush()
            except BaseException as e:
                logger.error(str(e))
                logger.error("Could not download dataset, skipping")
                continue
            #if archive - extract all, convert all to ntriples and merge into one file 
            #convert to ntriples and append .nt extension
            rdf2rdf = RDF2RDF()
            inputFile = datasetFilepath
            inputFormat = datasets[dataset].format
            outputFile = datasetFilepath + ".nt"
            (noConversion, extension, withErrors) = rdf2rdf.convert_to_ntriples(inputFile, inputFormat, outputFile)

            if(withErrors):
                logger.error("Dataset contains errors, skipping")
                shutil.rmtree(datasetFolder)
                continue

            if(noConversion):
                #File is already in nt or n3 format
                os.rename(datasetFilepath, datasetFilepath + extension)
            else:
                #remove old file
                os.remove(datasetFilepath)
            #pack with bz2
            self._compressFile(outputFile)


            ###Metadata processing
            datasetVoidTtl = datasets[dataset].void
            datasetMetaRdf = self.cache.getRdfMetadata(dataset)
            mergedMetadataString = self.mergeMetadata(dataset, datasetMetaRdf, datasetVoidTtl)
            datasetMetaFilepath = os.path.join(datasetFolder, configs[dataset].datasetId + ".meta.ttl")
            with open(datasetMetaFilepath, 'wb') as f:
                f.write(mergedMetadataString)
                f.flush()
            #compress with bz2
            self._compressFile(datasetMetaFilepath)

            ###Write config to file
            configFilepath = os.path.join(datasetFolder, configs[dataset].datasetId + ".cfg")
            with open(configFilepath, 'wb') as f:
                f.write(configs[dataset].toString())
                f.flush()
        logger.info("package creation complete!")

    def _compressFile(self, filename):
        command = "bzip2 "+ filename
        subprocess.Popen(command, shell=True)

    def mergeMetadata(self, dataset, dataset_meta_rdf, dataset_void_ttl):
        rdfparser = RdfParser()
        base_uri = "http://datahub,io/dataset/"+dataset
        dataset_meta_rdf_stream = rdfparser.init_stream_from_string(dataset_meta_rdf, base_uri, parser_name="rdfxml")
        dataset_meta_ttl_stream = rdfparser.init_stream_from_string(dataset_void_ttl, base_uri, parser_name="turtle")
        return rdfparser.merge_two_streams(dataset_meta_rdf_stream, dataset_meta_ttl_stream)
    
if __name__ == "__main__":
    print "hi"
