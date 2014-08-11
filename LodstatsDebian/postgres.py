from cfg import sqlalchemyUrl
import sqlalchemy

class LodstatsDB(object):
    def __init__(self):
        self.sqlalchemyUrl = sqlalchemyUrl
        self.db = sqlalchemy.create_engine(self.sqlalchemyUrl)

    def getResources(self):
        """
            Return resources, which are in the DataHub currently
        """
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

if __name__ == "__main__":
    ldb = LodstatsDB()
    print ldb.getCurrentResources()
    print "hi"
