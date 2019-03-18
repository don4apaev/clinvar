#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging, time
import argparse,  sys
import codecs
#sys.stderr = codecs.getwriter( 'utf8' )( sys.stderr )
#sys.stdout = codecs.getwriter( 'utf8' )( sys.stdout )

from defines import *
from parser import *

if __name__ == '__main__' :
    # Init parser
    parser = argparse.ArgumentParser( description = "Import XML or/and CSV ClinVar "\
                                     "files into clinvar database" )
    # Arguments description
    parser.add_argument( "--xml-files", "-x", help = "Path to xml files to import space separated",
                        default = [ XML_FILE ], required = False, nargs = '+'
                        )
    parser.add_argument( "--csv-files", "-c", help="Path to CSV files to import space separated",
                        default = [ CSV_FILE ], required=False, nargs='+'
                        )
    parser.add_argument( "--log-file", "-l", help="Path to log file",
                        default="{}_clinvar.log".format( time.strftime("%d_%m_%Y") ) 
                        )
    parser.add_argument( "--batch-size", "-b", type=int, help="Set batch parse size",
                        default=str( BATCH_SIZE )
                        )
    #parser.add_argument("--dbms", help="DBMS type to import into", default="MySQL")
    #parser.add_argument("--database", help="Database/Namespace", default="clinvar")
    #parser.add_argument("--host", help="DBMS host", default="localhost")
    #parser.add_argument("--port", help="DBMS port, defualt depends on DBMS type")
    #parser.add_argument("--user", "-u", help="DBMS user, defualt depends on DBMS type")
    #parser.add_argument("--password", "-p", help="DBMS password, defualt depends on DBMS type")
    #parser.add_argument("--options", "-o", help="Options to pass to database driver", nargs='*')
    # Parse arguments
    args = parser.parse_args( )
    # Init logging
    g_logger = logging.getLogger( 'clinvar' )
    g_handler = logging.FileHandler( args.log_file )
    g_formatter = logging.Formatter( '%(levelname)s@%(name)s:[%(asctime)s]>>> %(message)s' )
    g_handler.setFormatter( g_formatter )
    g_logger.addHandler( g_handler )
    g_logger.setLevel( logging.DEBUG )
    logger = logging.getLogger('clinvar.Main')
    # Greeting
    logger.info( "Hello.\nArguments: {}.".format( args ) )
    # Processing of each XML file
    for file in args.xml_files :
        logger.info( "Start ingesting XML file {}.".format( file ) )
        # Init and get batch generator
        batch_gen = (XML_File( file, REFERENCE_PATHS, ASSERTION_PATHS, TABLES_L ).
                        get_batch( args.batch_size ) )
        # Significance insertion, submitter accumulation
        sub_dict = { } # unique sumitter dict
        ic = 0 # insert counter
        t = tb = time.time( ) # rate time, batch rate time
        for batch_list in batch_gen:
            # Dict and list accumulation
            insert_list = []
            for d in batch_list:
                if d[ 0 ][ 0 ] not in sub_dict:
                    sub_dict.update( { d[ 0 ][ 0 ]:d[ 0 ][ 1 ] } )
                elif sub_dict[ d[ 0 ][ 0 ] ] != d[ 0 ][ 1 ] :
                    if sub_dict[ d[ 0 ][ 0 ] ] is None :
                        sub_dict[ d[ 0 ][ 0 ] ] = d[ 0 ][ 1 ]
                    elif d[ d [ 0 ][ 0 ] ] :
                        logger.debug( 'Name {} for {} known as {}.'.
                                        format( d[ 1 ], d[ 0 ], sub_dict[ d[ 0 ] ] )
                                    )
                insert_list += [ d [ 1 ] ]
            # Data base insertion
            ic += len( insert_list ) # insert( batch_list[ 0 ] )
            logger.debug( batch_list[ 0 ][ 0 ] )
            logger.info( 'Significance = {}; Batch Rate = {};  General rate = {}.'.
                            format( ic, len( batch_list[ 0 ] ) / ( time.time( ) - tb ), 
                                    ic / ( time.time( ) - t ) )
                        )
            tb = time.time( )
            #if ic > 3000:
            #    break
        # Submitter insertion
        sub_list = [ ] # unique submitter list
        ic = 0 # insert counter
        t = tb = time.time( ) # rate time, batch rate time
        for sid in sub_dict :
            sub_list += [ ( sid, sub_dict[ sid ] ) ]
            if len( sub_list ) == args.batch_size :
                # Data base insertion
                ic += len( sub_list ) # insert( sub_list )
                logger.debug( sub_list[ 0 ] )
                logging.info( 'Submitters = {}; Batch Rate = {};  General rate = {}.'.
                                format( ic, len( sub_list ) / ( time.time( ) - tb ), 
                                    ic / ( time.time( ) - t ) ) )
                del sub_list[ : ]
                tb = time.time( )
        logger.info( "Ingested {} in {}.".format( file, time.time( ) - t ) )
    # Processing of each CSV file
    for file in args.csv_files :
        logger.info( "Start ingest CSV file {}.".format( file ) )
        # Init and get batch generator
        batch_gen = CSV_File( file ).get_batch( args.batch_size )
        # Submission insertion
        ic = 0 # insert counter
        t = tb = time.time( ) # rate time, batch rate time
        for batch_list in batch_gen :
            # Data base insertion
            ic += len( batch_list ) # insert( batch_list )
            logger.debug( batch_list[ 0 ] )
            logger.info( 'Submissions = {}; Batch Rate = {};  General rate = {}.'.
                            format( ic, len( batch_list ) / ( time.time( ) - tb ), 
                                    ic / ( time.time( ) - t ) )
                        )
            tb = time.time( )
            #if ic > 3000:
            #    break
        logger.info( "Ingested {} in {}.".format( file, time.time( ) - t ) )
    # Farewell
    logger.info( "Bye bye.\n\n" )

