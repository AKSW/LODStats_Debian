import datetime
from MLStripper import strip_tags

from LogConfig import logger

class CkanDebianConfig(object):
    def __init__(self, package, resource):
        self.datasetId = package.name + "-datahubio"
        self.datasetName = package.title
        self.datasetDescription = ' '.join(strip_tags(package.notes_rendered).split())
        self.datasetDump = self.datasetId + ".nt.bz2"
        self.datasetMeta = self.datasetId + ".meta.ttl.bz2"
        self.datasetTripleCount = str(resource.triples)
        self.datasetUri = package.ckan_url
        self.datasetHomepage = package.url
        self.datasetDownloadUrl = resource.url
        #print resource.format
        self.datasetCopyrightHolder = package.author
        self.datasetCopyrightMail = package.author_email
        self.datasetCopyrightYear = str(datetime.datetime.strptime(package.metadata_modified, "%Y-%m-%dT%H:%M:%S.%f").year)
        self.datasetLicenseName = package.license_title
        try:
            self.datasetLicenseUrl = package.license_url
        except:
            self.datasetLicenseUrl = ''
        pass

    def toString(self):
        config = 'datasetId="%s"' % self.datasetId.encode('utf-8')
        config += "\n"
        config += 'datasetName="%s"' % self.datasetName.encode('utf-8')
        config += "\n"
        config += 'datasetDescription="%s"' % self.datasetDescription.encode('utf-8')
        config += "\n"
        config += 'datasetDump="%s"' % self.datasetDump.encode('utf-8')
        config += "\n"
        config += 'datasetMeta="%s"' % self.datasetMeta.encode('utf-8')
        config += "\n"
        config += 'datasetTripleCount="%s"' % self.datasetTripleCount.encode('utf-8')
        config += "\n"
        config += 'datasetUri="%s"' % self.datasetUri.encode('utf-8')
        config += "\n"
        config += 'datasetHomepage="%s"' % self.datasetHomepage.encode('utf-8')
        config += "\n"
        config += 'datasetDownloadUrl="%s"' % self.datasetDownloadUrl.encode('utf-8')
        config += "\n"
        try:
            config += 'datasetCopyrightHolder="%s"' % self.datasetCopyrightHolder.encode('utf-8')
            config += "\n"
        except:
            logger.error("No copyright holder for %s" % self.datasetId)
        try:
            config += 'datasetCopyrightMail="%s"' % self.datasetCopyrightMail.encode('utf-8')
            config += "\n"
        except:
            logger.error("No copyright mail for %s" % self.datasetId)
        try:
            config += 'datasetCopyrightYear="%s"' % self.datasetCopyrightYear.encode('utf-8')
            config += "\n"
        except:
            logger.error("No copyright year for %s" % self.datasetId)
        config += 'datasetLicenseUrl="%s"' % self.datasetLicenseUrl.encode('utf-8')
        config += "\n"
        config += 'datasetLicenseName="%s"' % self.datasetLicenseName.encode('utf-8')
        config += "\n"
        return config
