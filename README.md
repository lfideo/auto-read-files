# auto-read-files
The purpose of this repo is to demonstrate, how you can create a simple class to automate reading files from an sql database. The file `read_files.py`
contains the scipt that performs a file reading from a database with some basic functionaluty (as of now). 
Within the file is a class, named `ReadSQL`, which contains a method `read_sql_file`, that does the following:
- follows a specific procedure to establish connection with a database;
- reads in a table from a database, and drops a particular columns;
- performs some cleaning of column names, with the help of regex;
- makes all column names lower case.

After all steps are completed, the method returns a pandas dataframe, with which you can work onwards.

You can then import this class as a module to your working files, and escape the routine of writing the same code all over again.
