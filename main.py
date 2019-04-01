#!/usr/bin/python2
# -*- coding: utf-8 -*-

'''
ClinVar Ingester
Analyze the XML and/or CSV files and then writing parsing data into the MySQL database
in the tables for Submittion Significance, for match of the Submitters Names and IDs and variant_summary table.
'''

import logging, time
import argparse,  sys
#import codecs
#sys.stderr = codecs.getwriter( 'utf8' )( sys.stderr )
#sys.stdout = codecs.getwriter( 'utf8' )( sys.stdout )

from defines import *
from parser import  XML_File, CSV_File
from connect import Table

def main():
    # Init parser
    parser = argparse.ArgumentParser( prog = 'ClinVar Ingester', description = "Use -x and -c flags to parse XML and "\
                                     "CSV files, respectively.\nUse -s, -n and -v flags to write Significance, Name match "\
                                     " and variant_summary tables, respectively. Use -d flag to drop existing tables.\n"\
                                     "ex: mail.py -xsn" )
    # Arguments description
    parser.add_argument( "-x", help = "XML-file parsing flag", action='store_true' )
    parser.add_argument( "--xml-file", help = "Change path to XML file to import", default = XML_FILE )
    parser.add_argument( "-c", help = "CSV-file parsing flag", action='store_true' )
    parser.add_argument( "--csv-file", help = "Change path to CSV file to import", default = CSV_FILE )
    parser.add_argument( "-s", help = "Significance+ID+RCVAccession table write flag", action='store_true' )
    parser.add_argument( "--sig-table", help = "Change Significance+ID+RCVAccession table name", default = TABLE_SIG )
    parser.add_argument( "-n", help = "Submitter ID+Name table write flag", action='store_true' )
    parser.add_argument( "--name-table", help = "Change Submitter ID+Name table nane", default = TABLE_SUB )
    parser.add_argument( "-v", help = "variant_summary table write flag", action='store_true' )
    parser.add_argument( "--var_table", help = "Change variant_summary table name", default = TABLE_VAR )
    parser.add_argument( "-d", help = "Drop old tables, instead of rename", action='store_true' )
    parser.add_argument( "--log-file", "-l", help = "Change path to log file",
                        default = "{}_clinvar.log".format( time.strftime("%d_%m_%Y") ) )
    parser.add_argument( "--database", help = "Change Database/Namespace", default = DATABASE )
    parser.add_argument( "--port", type = int, help = "Change DBMS port", default =  PORT )
    parser.add_argument("--user", help = "Change DBMS user", default = USER )
    parser.add_argument("--password", help = "Change DBMS password", default = PASSWORD )
    # Parse arguments
    args = parser.parse_args( )
    # Init logging
    g_logger = logging.getLogger( 'clinvar' )
    g_handler = logging.FileHandler( args.log_file, mode='w', encoding='utf8' )
    g_formatter = logging.Formatter( '%(levelname)s@%(name)s:[%(asctime)s]>>> %(message)s' )
    g_handler.setFormatter( g_formatter )
    g_logger.addHandler( g_handler )
    g_logger.setLevel( logging.INFO )
    logger = logging.getLogger('clinvar.Main')
    # Greeting
    logger.info( "Hello." )
    logger.debug( args )#'{}\n\t{}'.format( args.xml_file, args.x_file ) )
    if args.x and ( args.s or args.n ) :
        # Processing of XML file
        logger.info( "Start ingesting XML file {}.".format( args.xml_file ) )
        # Init and get batch generator
        batch_gen = (XML_File( args.xml_file, REFERENCE_PATHS, ASSERTION_PATHS, TABLES_L ).
                           get_batch( BATCH_SIZE ) )
        if args.s :
            # Init database significance table
            sig = Table( args.database, args.port, args.user, args.password,
                       args.sig_table, COLUMN_SIG, TYPE_SIG, INDEX_SIG, True, True, args.d )
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
            if args.s :
                # Data base insertion
                ic += sig.insert( insert_list )
                logger.debug( '{}; {}'.format( insert_list[ 0 ], len(insert_list) ) )
                logger.info( 'Significance = {}; Batch Rate = {};  General rate = {}.'.
                            format( ic, len( insert_list ) / ( time.time( ) - tb ), 
                                    ic / ( time.time( ) - t ) )
                        )
            tb = time.time( )
        if args.n :
            # Init database submitters table
            sub = Table( args.database, args.port, args.user, args.password,
                    args.name_table, COLUMN_SUB, TYPE_SUB, INDEX_SUB, True, True, args.d )
            # Submitter insertion
            sub_list = [ ] # unique submitter list
            ic = 0 # insert counter
            t = tb = time.time( ) # rate time, batch rate time
            for sid in sub_dict :
                sub_list += [ ( sid, sub_dict[ sid ] ) ]
                if len( sub_list ) >= BATCH_SIZE :
                    # Data base insertion
                    ic += sub.insert( sub_list ) 
                    logger.debug( sub_list[ 0 ] )
                    logger.info( 'Submitters = {}; Batch Rate = {};  General rate = {}.'.
                                format( ic, len( sub_list ) / ( time.time( ) - tb ),
                                    ic / ( time.time( ) - t ) )
                                )
                    del sub_list[ : ]
                    tb = time.time( )
            ic += sub.insert( sub_list ) 
            logger.debug( sub_list[ 0 ] )
            logger.info( 'Submitters = {}; Batch Rate = {};  General rate = {}.'.
                        format( ic, len( sub_list ) / ( time.time( ) - tb ),
                                        ic / ( time.time( ) - t ) )
                        )
        logger.info( "Ingested {} in {}.".format( args.xml_file, time.time( ) - t ) )
    if args.c and args.v :
        # Processing of CSV file
        logger.info( "Start ingest CSV file {}.".format( args.csv_file ) )
        # Init and get batch generator
        batch_gen = CSV_File( args.csv_file ).get_batch( BATCH_SIZE )
        # Init database submissions table
        var = Table( args.database, args.port, args.user, args.password,
                       args.var_table, COLUMN_VAR, TYPE_VAR, INDEX_VAR, True, True, args.d )
        # Submission insertion
        ic = 0 # insert counter
        t = tb = time.time( ) # rate time, batch rate time
        for batch_list in batch_gen :
            # Data base insertion
            ic += var.insert( batch_list )
            logger.debug( batch_list[ 0 ] )
            logger.info( 'Submissions = {}; Batch Rate = {};  General rate = {}.'.
                            format( ic, len( batch_list ) / ( time.time( ) - tb ), 
                                    ic / ( time.time( ) - t ) )
                        )
            tb = time.time( )
        logger.info( "Ingested {} in {}.".format( args.csv_file, time.time( ) - t ) )
    # Farewell
    logger.info( "Bye bye." )


if __name__ == '__main__' :
    main()
