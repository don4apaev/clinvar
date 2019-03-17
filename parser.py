import logging, time
import StringIO, gzip
from lxml import etree

class Parsing_File:

    def __init__( self, file_name, batch_size ):
        self.file_name = file_name
        self.batch_size = batch_size
        # init logger
        self.logger = logging.getLogger('Main.Parser')

    def _file_init( self ):
        pass

    def _accum_block( self ):
        content = None
        line = self.file.readline( )
        if line != "":
            content = StringIO.StringIO( )
            print >> content, line
        return content

    def _process_block( self, content ):
        return tuple( content.rstrip().split() )

    def get_batch( self ):
        self.file = None
        with gzip.open(self.file_name,"rb") as self.file:
            self._file_init()
            list_of_insts = [ ] # start of file parse
            while True:
                block = None
                try:
                    block = self._accum_block( )
                    if block == None :
                        yield  list_of_insts
                        break
                    if len(list_of_insts) % self.batch_size == 0:
                        self.logger.debug( block.getvalue( ) )
                    list_of_insts += [self._process_block( block.getvalue( ) )]
                # any error in process_block( )
                except:
                    self.logger.error( 'In line {} (set {})'.format( self.file.tell( ),
                                                                    len(list_of_insts) ) )
                    raise
                if len(list_of_insts) % self.batch_size == 0 :
                    yield list_of_insts
                    del list_of_insts[:]

class XML_File( Parsing_File ):
    pass;

class CSV_File( Parsing_File ):

    def _file_init( self ):
        self.file.readline()
        
    def _process_block( self, content ):
        return tuple( content.rstrip().split('\t') )
