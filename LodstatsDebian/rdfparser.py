import RDF
from rdfnamespace import RDFNamespaces

class RdfParser(object):
    def __init__(self):
        self.namespaces = RDFNamespaces()

    def init_stream_from_string(self, string_rdf, base_uri, parser_name="rdfxml"):
        rdf_parser = RDF.Parser(name=parser_name)
        stream = rdf_parser.parse_string_as_stream(string_rdf, base_uri)
        return stream

    def merge_two_streams(self, stream_1, stream_2):
        model = RDF.Model()
        model.add_statements(stream_1)
        model.add_statements(stream_2)
        #serialize as ttl
        serializer = self.get_serializer()
        return serializer.serialize_model_to_string(model)

    def get_serializer(self):
        serializer = RDF.Serializer(name="turtle")
        serializer.set_namespace("rdf", self.namespaces.get_namespace('rdf'))
        serializer.set_namespace("void", self.namespaces.get_namespace('void'))
        serializer.set_namespace("void-ext", self.namespaces.get_namespace('void_ext'))
        serializer.set_namespace("qb", self.namespaces.get_namespace('qb'))
        serializer.set_namespace("dcterms", self.namespaces.get_namespace('dcterms'))
        serializer.set_namespace("ls-void", self.namespaces.get_namespace('ls_void'))
        serializer.set_namespace("ls-qb", self.namespaces.get_namespace('ls_qb'))
        serializer.set_namespace("ls-cr", self.namespaces.get_namespace('ls_cr'))
        serializer.set_namespace("xsd", self.namespaces.get_namespace('xsd'))
        serializer.set_namespace("xstats", self.namespaces.get_namespace('stats'))
        serializer.set_namespace("foaf", self.namespaces.get_namespace('foaf'))
        serializer.set_namespace("rdfs", self.namespaces.get_namespace('rdfs'))
        serializer.set_namespace("owl", self.namespaces.get_namespace('owl'))
        serializer.set_namespace("dcat", self.namespaces.get_namespace('dcat'))
        return serializer
