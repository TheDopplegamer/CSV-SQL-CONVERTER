# CSV-SQL-CONVERTER
Reads a csv file, and outputs it to an sqlite database
Before outputting the data, the program first removes all rows without 10 full rows of data, and outputs them to a new csv file.
It also creates a txt file showing the statistics of the conversion
Author: Abraham Blain

This repo contains the java class with the source code, and an executable .jar file. 


TO RUN:
-The jar file can be ran on any command line terminal.
-Make sure that an input csv file is inside the same directory as the jar file.
-Run the jar file as follows
  java -jar DatabaseConverter.jar <name of input file> <path of sqlite database> <Name of database table>
  (NOTE: The name of the input csv file should be entered without the ".csv" extension)
  ex. java -jar DatabaseConverter.jar my_data C:/Users/Documents/SQL/ My_Data_Table
  
 
This program uses the JDBC driver for the SQLite database, which can be found at https://bitbucket.org/xerial/sqlite-jdbc/src/default/


Once the input csv file is read, it splits each line into an array of 10 items, if more or less then 10 items are in the array, or if any array items are empty, the entire row is written to the bad.csv file. Otherwise, they are entered as text items into the database table.

This program was made under the assumption that it would be run on the command line, and that all csv data can be treated as text for the database.
