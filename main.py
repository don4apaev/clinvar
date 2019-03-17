#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging, time
import argparse,  sys
import codecs
sys.stderr = codecs.getwriter( 'utf8' )( sys.stderr )
sys.stdout = codecs.getwriter( 'utf8' )( sys.stdout )

from defines import *
from parser import *

if __name__ == '__main__':
    # init parser
    parser = argparse.ArgumentParser(description="Import XML or/and CSV ClinVar files into clinvar"\
                                     " database")
    # arguments description
    parser.add_argument("--xml-files", "-x", help="Path to xml files to import space separated",
                        default=[XML_FILE], required=False, nargs='+'
                        )
    parser.add_argument("--csv-files", "-c", help="Path to CSV files to import space separated",
                        default=[CSV_FILE], required=False, nargs='+'
                        )
    parser.add_argument("--log-file", "-l", help="Path to log file",
                        default="{}_clinvar.log".format( time.strftime("%d_%m_%Y") ) 
                        )
    parser.add_argument("--batch-size", "-b", type=int, help="Set batch parse size",
                        default=str(BATCH_SIZE)
                        )
    #parser.add_argument("--dbms", help="DBMS type to import into", default="MySQL")
    #parser.add_argument("--database", help="Database/Namespace", default="clinvar")
    #parser.add_argument("--host", help="DBMS host", default="localhost")
    #parser.add_argument("--port", help="DBMS port, defualt depends on DBMS type")
    #parser.add_argument("--user", "-u", help="DBMS user, defualt depends on DBMS type")
    #parser.add_argument("--password", "-p", help="DBMS password, defualt depends on DBMS type")
    #parser.add_argument("--options", "-o", help="Options to pass to database driver", nargs='*')
    # parse arguments
    args = parser.parse_args()
    # init logging
    logger = logging.getLogger('Main')
    handler = logging.FileHandler(args.log_file)
    formatter = logging.Formatter('%(levelname)s@%(name)s:[%(asctime)s]>>> %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    # greeting
    logger.info( "Hello.\nArguments: {}.".format(args) )
    # processing of each XML file
    for file in args.xml_files:
        #log start
        logger.info( "Start ingesting XML file {}.".format( file ) )
        '''
        batch_gen = Xml_File(file, BATCH_SIZE).get_batch()
        sub_dict = {}
        t = time.time()
        s1 = s2 = 0
        for batch_list in batch_gen:
            sub_list = [i for i in batch_list if type(i) == dict]
            set_list = [i for i in batch_list if type(i) == Set_Inst]
            var_list = [i for i in batch_list if type(i) == Var_Inst]
            for d in sub_list:
                for sid in d:
                    if sid not in sub_dict:
                        sub_dict.update({sid:d[sid]})
                    elif sub_dict[sid] != d[sid]:
                        if sub_dict[sid] is None:
                            sub_dict[sid] = d[sid]
                        elif d[sid]:
                            pass
            s1 += Sets.insert(set_list)
            s2 += Var.insert(var_list) 
            print 'Submitters=',len(sub_dict), ', Records=', s1,\
                  ', Submissions=', s2, ' Rate=', ((s1+s2)/(time.time() - t))
        t = time.time()
        sub_list = []
        for sid in sub_dict:
            inst = Sub_Inst()
            inst.SubmitterID = sid
            inst.SubmitterName = sub_dict[sid]
            sub_list += [inst] 
        s = Subs.insert(sub_list)
        logging.info( "Submitters= {}. Rate={}".format( s, (s/(time.time() - t) ) ) )
        '''
        logger.info( "Ingested {}.".format( file ) )
    # processing of each CSV file
    for file in args.csv_files:
        logger.info( "Start ingest CSV file {}.".format( file ) )
        batch_gen = CSV_File(file, args.batch_size).get_batch()
        s = 0
        t = tb = time.time() # rate time, batch rate time
        for batch_list in batch_gen:
            s += len(batch_list)
            logger.debug( batch_list[0] )
            logger.info( 'Submissions = {}; Batch Rate = {};  General rate = {}.'.
                    format( s, ( args.batch_size/(time.time() - tb) ), ( s/(time.time() - t) ) )
                    )
            tb = time.time()
            #if s > 3000:
            #    break
        logger.info( "Ingested {}.".format( file ) )
    # farewell
    logger.info( "Bye bye.\n\n" )

