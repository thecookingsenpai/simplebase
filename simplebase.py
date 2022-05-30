"""@package simplebase
@author TheCookingSenpai aka tcsenpai aka drotodev
@see github.com/thecookingsenpai
Documentation for simplebase noSQL database
"""

import os
import json

os.chdir(os.path.dirname(os.path.realpath(__file__)))

class simplebase():
    """
    The main class for simplebase: defines all the methods needed to create and interact with a database
    """

    ##Initialize the database object
    # @param self The database object
    # @param filename Optional boolean: if set to a string a db is opened (or created)
    def __init__(self, filename=None):
        ## @var The db object
        self.db = None
        if filename:
            self.open(filename)
    
    ##Helper to manage errors and warnings in a easy way
    # @param self The database object
    # @param msg The error message
    # @param exiting Optional boolean: if set to True the program will exit (also changes from ! to x the icon)
    def panic(self, msg, exiting=True):
        if exiting:
            severity = "x"
        else:
            severity = "!"
            
        print("[" + severity + "] " + str(msg))
        if exiting:
            os._exit(0)
    
    ##Open a database file and check the coherency of the main table
    # @param self The database object
    # @param file The absolute path of the database file
    def open(self, file):
        if not os.path.exists(file):
            self.db = {"_base_table_" : {"filename": file}}
            json.dump(self.db, open(file, "w+"))
        else:
            self.db = json.load(open(file))
            if not self.db.get('_base_table_'):
                self.panic("DB is malformed (missing _base_table_")
            if not self.db.get('_base_table_').get("filename"):
                self.panic("WARNING: Reinserting name in base table", exiting=False)
                self.db["_base_table_"]["filename"] = file        
                json.dump(self.db, open(self.db.get("_base_table_").get("filename")))
        
        print(self.db)
        return True
    
    ##Shows all the tables in the database
    # @param self The database object
    # @return False in case of error or a list containing all the tables names as strings
    def show_tables(self):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        tables = []
        for table in self.db:
            tables.append(table)
        return tables
    
    ##Shows all the columns in the table
    # @param self The database object   
    # @param tablename Name of the table to inspect
    # @return False in case of error or a list of strings with the columns names
    def show_columns(self, tablename):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False        
        if not self.db.get(tablename):
            self.panic("Table not found", exiting=False)
            return False
        columns = []
        for column in self.db.get(tablename):
            columns.append(column)
        return columns
    
    ##Shows the content of a table in the database
    # @param self The database object
    # @param tablename Name of the table to inspect
    # @return False in case of error or a dict representing the content of the db
    def load_table(self, tablename):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        if not "str" in type(tablename):
            self.panic("tablename must be a string", exiting=False)
            return False
        if not self.db.get(tablename):
            self.panic("Table not found", exiting=False)
            return False
        return self.db.get(tablename) # dict

    ##Creates a table (if not exists) in the database
    # @param self The database object
    # @param tablename Name of the table to work on
    # @param columns Optional list: creates all the columns available in the table
    # @return False in case of error or True if successful
    def create_table(self, tablename, columns=None):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        if self.db.get(tablename):
            self.panic("Table already found", exiting=False)
            return False
        self.db[tablename] = {"columns": None}
        if columns:
            if not "list" in str(type(columns)):
                self.panic("columns must be a list", exiting=False)
                return False
            self.db[tablename]["columns"] = columns
        json.dump(self.db, open(self.db.get("_base_table_").get("filename")))
        return True

    ##Shows a single row from a table
    # @param self The database object
    # @param tablename The name of the table to inspect
    # @param row The name of the row to load
    # @return False in case of error or a dict containing the requested row
    def load_row_from_table(self, tablename, row):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        if not self.db.get(tablename):
            self.panic("Table not found", exiting=False)
            return False
        if not self.db.get(tablename).get(row):
            self.panic("Row not found")
            return False
        return self.db.get(tablename).get(row) 

    ##Insert a single row in a table
    # @param self The database object
    # @param tablename The name of the table to inspect
    # @param row A dict containing the row to write (as: row = { name: "name", fields: {"field_1": value_1, ...} })
    # @return False in case of error or True if successful
    def add_row_to_table(self, tablename, row): 
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        if not self.db.get(tablename):
            self.panic("Table not found", exiting=False)
            return False
        if self.db.get(tablename).get(row.get("name")):
            self.panic("Row already found")
            return False
        columns = self.db.get(tablename).get('columns')
        if len(row.get("fields")) > len(columns):
            self.panic("Too many elements (" + str(len(row)) + ") for " + str(len(columns)) + " fields")
            return False
        self.db[tablename][row.get("name")] = {}
        for field in columns:
            if field in row.get("fields"):
                self.db[tablename][row.get("name")][field] = row.get("fields").get(field)
            else:
                self.db[tablename][row.get("name")][field] = None
        json.dump(self.db, open(self.db.get("_base_table_").get("filename")))
        return True
    
    ##Shows a column from a row in a table
    # @param self The database object
    # @param tablename The name of the table to inspect
    # @param row The name of the row to inspect
    # @param field The name of the column to inspect   
    # @return False in case of error or a dynamic type variable containing the column value     
    def load_field_from_row(self, tablename, row, field):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        if not self.db.get(tablename):
            self.panic("Table not found", exiting=False)
            return False
        if not self.db.get(tablename).get(row):
            self.panic("Row not found")
            return False
        if not self.db.get(tablename).get(row).get(field):
            self.panic("Field not found")
            return False
        return self.db.get(tablename).get(row).get(field) # variable type

    ##Update the value of a column in a row in a table
    # @param self The database object
    # @param tablename The name of the table to work on
    # @param row The name of the row to work on
    # @param field The name of the column to update
    # @param value The new value of the column
    # @return False in case of error or True if successful      
    def update_field_to_row(self, tablename, row, field, value):
        if not self.db:
            self.panic("DB not opened", exiting=False)
            return False
        if not self.db.get(tablename):
            self.panic("Table not found", exiting=False)
            return False
        if not self.db.get(tablename).get(row):
            self.panic("Row not found", exiting=False)
            return False
        if not self.db.get(tablename).get('columns'):
            self.panic("Field not found", exiting=False)
            return False
        if not type(self.db.get(tablename).get(row).get(field)) == type(value):
            self.panic("WARNING: Type has changed!", exiting=False)
        json.dump(self.db, open(self.db.get("_base_table_").get("filename")))
        return True
            