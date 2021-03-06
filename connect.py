import logging, time
import mysql.connector

class Connection:
    def __init__( self, database = None, port = None, user = None,
                    password = None, connect_now = False ) :
        self.database = database
        self.user = user
        self.password = password
        if ( port ) :
            self.port = port
        else :
            self.port = 3306
        self.logger = logging.getLogger( 'clinvar.Connect' )
        if ( connect_now ) :
            self.connect( )

    def connect_dbms( self, port, user, password, database  ) :
        connection = mysql.connector.connect(
            host        = "localhost",
            user        = user,
            passwd      = password,
            database    = database,
            port        = port
        )
        return connection

    def connect( self ) :
        self.connection = self.connect_dbms( self.port, self.user, self.password, self.database )
        self.connection.autocommit = True

    def get_connection( self ) :
        connection = self.connect_dbms( self.port, self.user, self.password, self.database )
        connection.autocommit = True
        return connection

    def is_table_exist( self, table ) :
        c = self.connection.cursor( )
        p = self.parameter( )
        t = table.split( '.' )
        if ( len( t ) > 1 ) :
            table = t[ 1 ]
            schema = t[ 0 ]
        else:
            schema = None
        sql = "SELECT table_name FROM information_schema.tables "\
                    "WHERE table_schema = {} AND table_name = {}"
        if not schema :
            schema = self.database
        sql = sql.format( p, p )
        c.execute( sql, (schema, table) )
        rows =  c.fetchall( )
        exists = len( rows ) > 0
        c.close( )
        return exists

    def is_connected( self ) :
        return self.connection and self.connection.is_connected( )

    def parameter(self):
        return '%s'

    def quote( self, string ) :
        return "`{}`".format( string )

    def create_table( self, table, columns, types, drop = False ) :
        c = self.connection.cursor()
        column_string = ", ".join( ["{} {}".format( self.quote( column ), type_ )
                                    for column, type_ in zip( columns, types ) ] 
                                )
        if ( self.is_table_exist( table ) > 0 ) :
            if drop :
                sql = "DROP TABLE {}".format( table )
            else :
                table_old = table + '_OLD'
                if ( self.is_table_exist( table_old ) > 0 ) :
                    sql = "DROP TABLE {}".format( table_old )
                    self.logger.debug( sql )
                    c.execute( sql )
                sql = "RENAME TABLE {} TO {}".format( table, table_old )
            self.logger.debug(sql)
            c.execute( sql )
        sql = "CREATE TABLE {} ({})".format( table, column_string )
        self.logger.debug( sql )
        c.execute( sql )

    def create_index( self, table, name, columns, unique = False, btree = False ) :
        c = self.connection.cursor( )
        qualifier       = "UNIQUE" if unique else ""
        btree           = "USING BTREE" if btree else ""
        column_string   = ",".join( [ self.quote( cl ) for cl in columns ] )
        sql             = "CREATE {q} INDEX {name} ON {table} ({cols}) {ubt}".format(
                                q = qualifier, name = self.quote( name ), table = table, 
                                cols = column_string, ubt = btree )
        self.logger.debug( sql )
        c.execute( sql )

    def close( self ) :
        if ( self.is_connected( ) ) :
            self.connection.close( )

    def __exit__( self, exc_type, exc_val, exc_tb ) :
        self.close( )

    def __enter__( self ) :
        return self

class Table( Connection ) :
    def __init__( self, database = None, port = None, user = None, password = None,
            table = None, columns = None, types = None, indexes = None,
            connect_now = False, create = False, drop = False ) :
        Connection.__init__( self, database, port, user, password, connect_now )
        self.table = table
        self.columns = columns
        self.types = types
        self.indexes = indexes
        self.drop = drop
        self.insert_sql = "INSERT INTO {} ({}) VALUES ({})".format( self.table, 
                ", ".join( [ self.quote( c ) for c in self.columns ] ), 
                ", ".join( [ self.parameter( ) for c in self.columns ] ) 
                )
        if connect_now and create :
            self.create_table()
            self.create_index()

    def create_table( self ) :
        Connection.create_table( self, self.table, self.columns, self.types, self.drop )

    def create_index( self ) :
        for i in self.indexes :
            Connection.create_index( self, self.table, i[0], i[1], i[2], i[3] )

    def insert ( self, batch ) :
        rowcount = 0
        if ( not self.is_connected( ) ) :
            self.connect( )
            #raise( BaseException( "connect error" ) )
        cursor = self.connection.cursor( )
        if ( len( batch ) == 1 ):
            cursor.execute( self.insert_sql, batch[0] )
            rowcount += cursor.rowcount
        else:
            cursor.executemany( self.insert_sql, batch  )
            rowcount += cursor.rowcount
        cursor.close()
        return rowcount

        
