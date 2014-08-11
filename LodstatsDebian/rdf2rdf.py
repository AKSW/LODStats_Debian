import re
import subprocess
from log import logger

class RDF2RDF(object):
    def __init__(self):
        pass

    def convert_to_ntriples(self, input_file, input_format, output_file):
        with_errors = False
        if(re.match(r'ttl', input_format, re.I | re.M) or
           re.match(r'turtle', input_format, re.I | re.M)):
            i = "turtle"
        elif(re.match(r'n3', input_format, re.I | re.M)):
            #No conversion here
            logger.info("The file %s has n3 serialization" % input_file)
            return (True, ".nt", with_errors)
        elif(re.match(r'ntriples', input_format, re.I | re.M) or
             re.match(r'nt', input_format, re.I | re.M)):
            #No conversion required
            return (True, ".nt", with_errors)
        else:
            i = "rdfxml"
        command = "rapper -i %s -o ntriples %s" % (i, input_file)

        output_file_handler = open(output_file, 'a+b')
        pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = pipe.communicate()

        stderr_string = str(stderr)
        logger.error(stderr_string)

        if(re.match(r'.*error', stderr_string, re.I|re.M|re.S)):
            with_errors = True

        for data in stdout:
            output_file_handler.write(data)

        return (False, ".nt", with_errors)
