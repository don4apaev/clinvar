import logging, time
import StringIO, gzip
from lxml import etree

class Parsing_File:

    def __init__( self, file_name, batch_size ):
        self.file_name = file_name
        self.batch_size = batch_size
        # init logger
        self.logger = logging.getLogger('Parser')

    def _file_init( self ):
        pass

    def _accum_block( self ):
        content = None
        line = self.file.readline( )
        if line != "":
            content = StringIO.StringIO( )
            print >> content, line.rstrip( )
        return content

    def _process_block( self, content ):
        return tuple( content.split() )

    def get_batch( self ):
        self.file = None
        with gzip.open(self.file_name,"rb") as self.file:
            self._file_init()
            list_of_insts = [ ] # start of file parse
            while True:
                block = None
                try:
                    s += 1
                    block = self._accum_block( )
                    if block == None :
                        yield  list_of_insts
                        break
                    list_of_insts += self._process_block( block.getvalue( ) )
                    if ( len(list_of_insts) % self.batch_size == 0 ):
                        
                # any error in process_block( )
                except:
                    self.logger.error( 'In line {} (set {})'.format( self.file.tell( ),
                                                                    len(list_of_insts) ) )
                    raise
                if ( len(list_of_insts) % self.batch_size == 0 ):
                    self.logger.debug( block.getvalue( ) )
                    yield list_of_insts
                    del list_of_insts[:]

class XML_File( Parsing_File ):
    pass;

class CVS_File( Parsing_File ):

    def _accum_block( self ):
        content = None
        line = self.file.readline( )
        if line != "":
            content = StringIO.StringIO( )
            print >> content, line.rstrip( )
        return content

    def _process_block( self, content ):
        return tuple( content.split('\t') )
