import logging, time
import StringIO, gzip
from lxml import etree

class Parsing_File:
    def __init__( self, file_name, batch_size ):
        self.file_name = file_name
        self.batch_size = batch_size
        # init logger
        self.logger = logging.getLogger('Parser')

    def get_batch( self ):
        # batch counter
        s = 0
        t0 = time.time( )
        self.file = None
        with gzip.open(self.file_name,"rb") as self.file:
            # start of file parse
            list_of_insts = [ ]
