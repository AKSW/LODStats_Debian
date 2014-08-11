import sqlalchemy
import cPickle
import copy
import os
import shutil
import re
import requests
import subprocess
from CkanDebianConfig import CkanDebianConfig
from rdf2rdf import RDF2RDF

from LogConfig import logger
from rdfparser import RdfParser
from cfg import sqlalchemyUrl


class LodstatsDebian(object):
    def __init__(self):
        self.sqlalchemyUrl = sqlalchemyUrl
        self.db = sqlalchemy.create_engine(self.sqlalchemyUrl)
        self.current_resources = self.load_resources_from_cache()
        self.datasets = self.group_resource_by_package(self.current_resources)
        self.datasets_no_duplicates = self.filter_out_duplicate_resources(self.datasets)
        self.datasets_less_than_million_triples = self.filter_datasets_more_than_million_triples(self.datasets_no_duplicates)
        self.datasets_less_than_one_dump = self.filter_datasets_more_than_one_dump(self.datasets_less_than_million_triples)
        self.datasets_with_free_licenses = self.filter_datasets_no_free_licenses(self.datasets_less_than_one_dump)
        self.datasets_with_dumps_only = self.filter_datasets_meta_examples_api(self.datasets_with_free_licenses)
        self.datasets_with_one_resource = self.filter_to_one_resource(self.datasets_with_dumps_only)
        self.datasets_final = self.datasets_with_one_resource
        self.dataset_configs = self.generate_configs(self.datasets_final)
        self.create_datasets(self.datasets_final, self.dataset_configs)

    def get_dataset_metadata(self, dataset):
        dataset_meta = self.dataset_meta_get(dataset)
        if(not dataset_meta):
            url = "http://datahub.io/dataset/"+dataset+".rdf"
            r = requests.get(url)
            dataset_meta = r.content
            self.dataset_meta_put(dataset, dataset_meta)
        return dataset_meta

    def merge_metadata(self, dataset, dataset_meta_rdf, dataset_void_ttl):
        rdfparser = RdfParser()
        base_uri = "http://datahub,io/dataset/"+dataset
        dataset_meta_rdf_stream = rdfparser.init_stream_from_string(dataset_meta_rdf, base_uri, parser_name="rdfxml")
        dataset_meta_ttl_stream = rdfparser.init_stream_from_string(dataset_void_ttl, base_uri, parser_name="turtle")
        return rdfparser.merge_two_streams(dataset_meta_rdf_stream, dataset_meta_ttl_stream)

    def create_datasets(self, datasets, configs):
        for dataset in datasets:
            logger.info("Creating debian config for %s" % dataset)
            dataset_folder = "datasets/"+dataset
            if not os.path.exists(dataset_folder):
                os.makedirs(dataset_folder)

            if os.path.isfile(dataset_folder + '/' + configs[dataset].datasetId + ".cfg"):
                logger.info("debian config already exists, skipping")
                continue

            ###Download dataset RDF data
            try:
                r = requests.get(datasets[dataset].url, stream=True, timeout=1)
                dataset_filepath = os.path.join(dataset_folder, configs[dataset].datasetId)
                with open(dataset_filepath, 'wb') as f:
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
            input_file = dataset_filepath
            input_format = datasets[dataset].format
            output_file = dataset_filepath + ".nt"
            (no_conversion, extension, with_errors) = rdf2rdf.convert_to_ntriples(input_file, input_format, output_file)

            if(with_errors):
                logger.error("Dataset contains errors, skipping")
                shutil.rmtree(dataset_folder)
                continue

            if(no_conversion):
                #File is already in nt or n3 format
                os.rename(dataset_filepath, dataset_filepath + extension)
            else:
                #remove old file
                os.remove(dataset_filepath)
            #pack with bz2
            self.compress_file(output_file)


            ###Metadata processing
            dataset_void_ttl = datasets[dataset].void
            dataset_meta_rdf = self.get_dataset_metadata(dataset)
            merged_metadata_string = self.merge_metadata(dataset, dataset_meta_rdf, dataset_void_ttl)
            dataset_meta_filepath = os.path.join(dataset_folder, configs[dataset].datasetId + ".meta.ttl")
            with open(dataset_meta_filepath, 'wb') as f:
                f.write(merged_metadata_string)
                f.flush()
            #compress with bz2
            self.compress_file(dataset_meta_filepath)

            ###Write config to file
            config_filepath = os.path.join(dataset_folder, configs[dataset].datasetId + ".cfg")
            with open(config_filepath, 'wb') as f:
                f.write(configs[dataset].toString())
                f.flush()
        logger.info("package creation complete!")

    def compress_file(self, filename):
        command = "bzip2 "+ filename
        subprocess.Popen(command, shell=True)

    def filter_datasets_meta_examples_api(self, datasets):
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

        datasets_wiped = self.purge_resources(datasets_copy)
        return datasets_wiped

    def generate_configs(self, datasets):
        configs = {}
        for dataset in datasets:
            package = self.get_dataset(dataset)
            config = CkanDebianConfig(package, datasets[dataset])
            configs[dataset] = config
        return configs

    def filter_to_one_resource(self, datasets):
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

    def get_current_resources_from_db(self):
        results = []
        query = "select rdfdoc.uri, stat_result.triples, stat_result.void \
                from rdfdoc, stat_result \
                where stat_result.rdfdoc_id=rdfdoc.id \
                and rdfdoc.in_datahub=true \
                and stat_result.triples > 0"
        results_generator = self.db.execute(query);
        for row in results_generator:
            results.append(row)
        return results

    def filter_out_duplicate_resources(self, datasets):
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
        datasets_wiped = self.purge_resources(datasets_copy)
        datasets_purged = self.purge_datasets(datasets_wiped)

        return datasets_purged

    def purge_resources(self, datasets):
        datasets_wiped = {}
        for dataset in datasets:
            for resource in self.ifilter(lambda x: x == {}, datasets[dataset]):
                if(datasets_wiped.get(resource.package_name, False)):
                    datasets_wiped[resource.package_name].append(resource)
                else:
                    datasets_wiped[resource.package_name] = [resource]
        return datasets_wiped

    def purge_datasets(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        to_purge = []
        for dataset in datasets_copy:
            if(len(datasets_copy[dataset]) == 0):
                to_purge.append(dataset)

        for dataset in to_purge:
            del(datasets_copy[dataset])

        return datasets_copy

    def ifilter(self, predicate, iterable):
        if predicate is None:
            predicate = bool
        for x in iterable:
            if not predicate(x):
                yield x

    def filter_datasets_no_free_licenses(self, datasets):
        datasets_copy = copy.deepcopy(datasets)
        list_to_delete = []

        for dataset in datasets_copy:
            #Try to load from cache
            package = self.get_dataset(dataset)
            if(not package.isopen):
                list_to_delete.append(dataset)

        for dataset in list_to_delete:
            del(datasets_copy[dataset])

        return datasets_copy

    def get_dataset(self, dataset_name):
        from csv2rdf.ckan.package import Package
        package = {}
        if(os.path.isfile('package-cache/'+dataset_name)):
            package = self.package_cache_get(dataset_name)
        else:
            package = Package(dataset_name)
            del(package.ckan)
            logger.info("Dumping package %s" % dataset_name)
            self.package_cache_put(package)
        return package

    def dataset_meta_put(self, package_name, meta):
        cPickle.dump(meta, open('dataset-meta/'+package_name, 'wb'), -1)

    def dataset_meta_get(self, package_name):
        if(os.path.isfile("dataset-meta/"+ package_name)):
            return cPickle.load(open('dataset-meta/'+package_name, 'rU'))
        else:
            return False

    def package_cache_put(self, package):
        cPickle.dump(package, open('package-cache/'+package.name, 'wb'), -1)

    def package_cache_get(self, package_name):
        return cPickle.load(open('package-cache/'+package_name, 'rU'))

    def filter_datasets_more_than_one_dump(self, datasets):
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

    def filter_datasets_more_than_million_triples(self, datasets):
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

    def group_resource_by_package(self, resources):
        datasets = dict()
        for num, resource in enumerate(resources):
            try:
                if(datasets.get(resource.package_name, False)):
                    datasets[resource.package_name].append(resource)
                else:
                    datasets[resource.package_name] = [resource]
            except BaseException as e:
                logger.error("Resource does not exist: %s" % str(e)) 
        return datasets

    def load_resources_from_cache(self):
        from os import listdir
        from os.path import isfile, join
        onlyfiles = [ f for f in listdir('cache/') if isfile(join('cache/',f)) ]
        resources = []
        for f in onlyfiles:
            st = open('cache/'+f, 'rU')
            res = cPickle.load(st)
            resources.append(res)
        return resources

    def get_resource_by_uri(self, uri):
        import csv2rdf.ckan.resource
        resource = csv2rdf.ckan.resource.Resource('')
        try:
            resource.init_from_uri(uri)
            logger.info("Resource %s - SUCCESS" % uri.encode('utf-8'))
        except BaseException as e:
            logger.error("Cannot load resource: %s - %s" % (uri.encode('utf-8'), str(e)))
        return resource

    def write_obj_to_file(self, obj, filename):
        cPickle.dump(obj, open('cache/'+filename, 'wb'))

    def update_cache(self):
        current_resources_from_db = self.get_current_resources_from_db()
        dump = []
        length = len(current_resources_from_db)
        i = 0
        for (uri, triples, void) in current_resources_from_db:
            logger.info("Dumping %s out of %s" % (str(i), str(length)))
            i += 1
            resource = self.get_resource_by_uri(uri)
            resource.triples = triples
            resource.void = void
            self.write_obj_to_file(resource, "resource"+str(i))
            dump.append(resource)
        return dump

if __name__ == "__main__":
    lodstatsdebian = LodstatsDebian()
    #import ipdb; ipdb.set_trace()
