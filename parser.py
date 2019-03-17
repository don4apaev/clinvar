import logging, time
import StringIO, gzip
from lxml import etree

class Parsing_File:

    def __init__( self, file_name, batch_size ):
        # set variables
        self.file_name = file_name
        self.batch_size = batch_size
        # init logger
        self.logger = logging.getLogger('Main.Parser')

    def _file_init( self ):
        pass

    def _get_block( self ):
        content = None
        # get line
        line = self.file.readline( )
        # end of file: line == "" -> return None
        if line != "":
            content = StringIO.StringIO( )
            print >> content, line
        return content

    def _process_block( self, content ):
        # split by spaces
        return [tuple( content.rstrip().split() )]

    def get_batch( self ):
        self.file = None
        # unzip and open file
        with gzip.open(self.file_name,"rb") as self.file:
            # preparse init
            self._file_init()
            list_of_tuples = [ ]
            # start of file parse
            while True:
                block = None
                try:
                    # get file block
                    block = self._get_block( )
                    # end of file
                    if block == None :
                        yield  list_of_tuples
                        break
                    # accumulate values batch
                    list_of_tuples += self._process_block( block.getvalue( ) )
                # any error while work with block
                except:
                    self.logger.error( 'In line {} (set {})'.format( self.file.tell( ),
                                                                    len(list_of_tuples) ) )
                    raise
                # yield batch
                if len(list_of_tuples) % self.batch_size == 0 :
                    yield list_of_tuples
                    del list_of_tuples[:]


# XML parser
class XML_File( Parsing_File ):

    def __init__( self, file_name, batch_size, ref_path, ass_path ):
        Parsing_File.__init__( self, file_name, batch_size )
        # additional variable
        self.ref_path = ref_path # ReferenceClinVarAssertion paths
        self.ass_path = ass_path # ClinVarAssertion paths

    def _get_block( self ):
        content = None
        # accumulate block
        while True:
            line = self.file.readline( )
            if line == "":
                # end of file
                break
            elif "<ClinVarSet ID=" in line:
                # start of ClinVar Set Block
                assert content is None
                content = StringIO.StringIO( )
                print >> content, line.rstrip( )
            elif "</ClinVarSet>" in line:
                # end of ClinVar Set Block
                print >> content, line.rstrip( )
                break
            elif content:
                # body of ClinVar Set Block
                print >> content, line.rstrip( )
        return content

    def _process_block( self, content ):
        list_of_values = []
        # init XML parser
        parser = etree.XMLParser(remove_comments = True)
        root = etree.fromstring(content,parser)
        # take referense values
        nodes = root.xpath("/ClinVarSet/ReferenceClinVarAssertion")
        if len(nodes) != 1:
            self.logger.error( '{} ReferenceClinVarAssertion elements'.format( len(nodes) ) )
            raise( BaseException("Unnormal ReferenceClinVarAssertion element") )
        ref_list = []
        for n in nodes:
            for path in self.nodes_path:
                # for XML attribute values
                if type(path) == tuple:
                    subn = n.xpath(path[0])
                    ref_list += [subn[0].get(path[1])]
                # for XML content values
                else:
                    subn = n.xpath(path)
                    ref_list += [subn[0].text]
        # take assertion values
        nodes = root.xpath("/ClinVarSet/ClinVarAssertion")
        ass_list = []
        for n in nodes:
            int_list = []
            for path in self.nodes_path:
                # for XML attribute values
                if type(path) == tuple:
                    subn = n.xpath(path[0])
                    int_list += [subn[0].get(path[1])]
                # for XML content values
                else:
                    subn = n.xpath(path)
                    int_list += [subn[0].text]
            ass_list += [int_list]
        


#CSV parser
class CSV_File( Parsing_File ):

    def _file_init( self ):
        # skip first line with values names
        self.file.readline()
        
    def _process_block( self, content ):
        # split by tab
        return [tuple( content.rstrip().split('\t') )]
