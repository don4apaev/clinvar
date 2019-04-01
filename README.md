# ClinVar Ingester

## Overview

ClinVar Ingester designed to analyze the XML and/or CSV files and then writing parsing data into the MySQL database in the tables for Submittion Significance, for match of the Submitters Names and IDs and/or variant_summary table.

## Usage

For parse XML file use -x flag .
For parse CVS file use -c flag.
If was parse XML file then use -s flag for write Submittions Significance table and/or -n flag for write  Submitters Name match table.
If was parse CSV file then use -v flag for write variant_summary table.
There is no sense of using last 3 flags without files parse. Flags can be combined.
Default XML and CSV files and Significance, Name, Variant_summary tables names, database namespace, port, user and password is stored in 'defines.py' file. This values can be changed with --xml-file, --csv-file, --sig-table, --name-table, --var_table, --database, --port, --user, --password options, respectively.
Default log file is written to cwd with current date and '_clinvar.log' suffix in name. Log file path can be changed with --log-file option.
If the database already contains writable table, ClinVar Ingester will rename it with adding '_OLD' to an old name. If database already contains an '_OLD' table, it will be dropped. If -d flag is used, existing writable table will be dropped instead of renaming.
