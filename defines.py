CSV_FILE = "/data/store/szubarev/Downloads/variant_summary.txt.gz"
XML_FILE = "/data/store/szubarev/Downloads/ClinVarFullRelease_00-latest.xml.gz"

DATABASE = "clinvar"
TABLE_SUB = "CV_Submitters_T"
TABLE_SET = "ClinVar2Sub_Sig_T"
TABLE_VAR = "variant_summary_T"

COLUMN_SUB = (  "SubmitterName",
                "SubmitterID"
                )
TYPE_SUB = (    'text',
                'INT'
                )
COLUMN_SET = (  "SubmitterID",
                "RCVaccession",
                'ClinicalSignificance'
                #"Assembly",
                #"VariationID",
                )
TYPE_SET = (    'INT',
                'varchar(12)',
                'text'
                #'varchar(4)',
                #"int'
                )
COLUMN_VAR = (  '#AlleleID',
                'Type',
                'Name',
                'GeneID',
                'GeneSymbol',
                'HGNC_ID',
                'ClinicalSignificance',
                'ClinSigSimple',
                'LastEvaluated',
                'RS# (dbSNP)',
                'nsv/esv (dbVar)',
                'RCVaccession',
                'PhenotypeIDS',
                'PhenotypeList',
                'Origin',
                'OriginSimple',
                'Assembly',
                'ChromosomeAccession',
                'Chromosome',
                'Start',
                'Stop',
                'ReferenceAllele',
                'AlternateAllele',
                'Cytogenetic',
                'ReviewStatus',
                'NumberSubmitters',
                'Guidelines',
                'TestedInGTR',
                'OtherIDs',
                'SubmitterCategories',
                'VariationID'
                )
TYPE_VAR = (    'int(11) DEFAULT NULL',
                'text',
                'text',
                'int(11) DEFAULT NULL',
                'text',
                'text',
                'text',
                'int(11) DEFAULT NULL',
                'text',
                'int(11) DEFAULT NULL,',
                'text',
                'text',
                'text',
                'text',
                'text',
                'text',
                'text',
                'text',
                'varchar(12) DEFAULT NULL',
                'int(11) DEFAULT NULL',
                'int(11) DEFAULT NULL',
                'text',
                'text',
                'text',
                'text',
                'int(11) DEFAULT NULL',
                'text',
                'text',
                'text',
                'int(11) DEFAULT NULL',
                'int(11) DEFAULT NULL'
                )

INDEX_SUB = (   "UNIQUE INDEX Index0 ON {}.{} (SubmitterID)", ) # .format(DATABASE, TABLE_VAR)
INDEX_SET = (   "UNIQUE INDEX Index0 ON {}.{} (RCVaccession, SubmitterID)",
                "INDEX Index1 ON {}.{} (SubmitterID, RCVaccession)" )
INDEX_VAR = (   'INDEX index1 ON {}.{} (#AlleleID) USING BTREE', 
                'INDEX c_idx ON {}.{} (Chromosome) USING BTREE', 
                'INDEX p_idx ON {}.{} (Start, Stop) USING BTREE', 
                'INDEX Cs0_idx ON {}.{} (ClinSigSimple) USING BTREE' )

BATCH_SIZE = 1000
