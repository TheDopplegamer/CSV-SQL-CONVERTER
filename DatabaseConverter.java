
package databaseconverter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Abraham
 */
public class DatabaseConverter {

    /**
     * Connect to a sample database
     *
     * @param fileName the database file name
     * @param address
     */
    public void createNewDatabase(String fileName,String address) {
 
        String url = address + fileName;
 
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }
 
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
 
    
    /**
     * Create a new table in the test database
     *
     * @param url
     * @param table_name
     */
    public void createNewTable(String url,String table_name) {

        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS "+table_name+ " (\n"
                + "	A text,\n"
                + "	B text,\n"
                + "	C text,\n"
                + "	D text,\n"
                + "	E text,\n"
                + "	F text,\n"
                + "	G text,\n"
                + "	H text,\n"
                + "	I text,\n"
                + "	J text\n"
                + ");";
        
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    
    /**
     * Connect to the database
     *
     * @return the Connection object
     */
    private Connection connect(String url) {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
    
    
    
    /**
     * Insert a new row into a table
     *
     * @param values
     *   The entries of a row
     * @param url
     *   The address of the sqlite database
     * @param table
     *   The name of the database table
     */
    public void insert(String[] values,String url, String table) {
        
        String sql = "INSERT INTO "+table+"(A,B,C,D,E,F,G,H,I,J) VALUES(?,?,?,?,?,?,?,?,?,?)";
 
        try (Connection conn = this.connect(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) 
        {
            for(int i = 1; i <= 10;i++){
                pstmt.setString(i, values[i-1]);
            }
                pstmt.executeUpdate();
        } 
        catch (SQLException e) 
        {
                System.out.println(e.getMessage());
        }
    }
    
    //When a row of data cannot be entered into the database, enter it into a cvs file
    public void Write_Bad_Entry(String[] values, FileWriter fw){
        String bad = "";
        for(int i = 0;i < 10;i++){
            if(i < values.length){
                bad += values[i];
            }
            if(i < 9){
                bad += ",";
            }
            else{
                bad += "\n";
            }
        }
        try {
            fw.append(bad);
        } catch (IOException ex) {
            Logger.getLogger(DatabaseConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    
    /**
     * @param args the command line arguments
     *    0 - name of the input csv file(ex. "test" for test.csv)
     *    1 - address of the database (ex. "C:/Users/user/documents/sql/"
     *    2 - The name of the database table
     */
    public static void main(String[] args) {
        
        if(args.length != 3){
            System.out.println("ERROR, NEED 3 ARGUMENTS:");
            System.out.println("   NAME OF INPUT CSV FILE (WITHOUT .csv exstension");
            System.out.println("   ADDRESS OF DATABASE");
            System.out.println("   NAME OF DATABASE TABLE");
            return;
        }
        
        
        
        
        DatabaseConverter db = new DatabaseConverter();
        
        FileWriter bad_file = null;
        
        int num_recieved = 0;
        int num_success = 0;
        int num_failure = 0;
        
        String fileName = args[0] + ".db";
        String address = "jdbc:sqlite:"+args[1];
        String table = args[2];
        
        
        try {
            bad_file = new FileWriter(args[0]+"-bad.csv");
        } catch (IOException ex) {
            Logger.getLogger(DatabaseConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        //Create the database and table, or connect to it if it already exists
        
        String url = address+fileName;
        db.createNewDatabase(fileName,address);
        db.createNewTable(url,table);
        System.out.println("READING FILE");
        //Read the cvs file line by line
        try (BufferedReader br = new BufferedReader(new FileReader(args[0]+".csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                //Prevent the collumn header from being read
                if("A,B,C,D,E,F,G,H,I,J".equals(line)){
                    line = br.readLine();
                    if(line == null){
                        return;
                    }
                }
                //Split the line into values that are loaded into an array
                String[] values = line.split(",(?=([^\"]|\"[^\"]*\")*$)");
                num_recieved++;
                //Only insert it into the database if there is 10 items
                if(values.length == 10){
                    //Prevent insertion if any of the items are empty
                    boolean empty_found = false;
                    for(int i = 0;i < 10;i++){
                        if(values[i].length() == 0){
                           empty_found = true;
                           break;
                        }
                    }
                    //Insert the values into a new row of the database table
                    if(!empty_found){
                        db.insert(values,url,table);
                        num_success++;
                    }
                    //Otherwise, insert the values into a bad cvs file
                    else{
                        db.Write_Bad_Entry(values,bad_file);
                        num_failure++;
                    }
                }
                //Otherwise, insert the values into a bad cvs file
                else{
                    db.Write_Bad_Entry(values,bad_file);
                    num_failure++;
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println("CSV FILE NOT FOUND");
        } catch (IOException ex) {
            System.out.println("IO ERROR");
        }
        
        //Write the statistics to a log file
        FileWriter log_file = null;
        System.out.println("FINISHED!");
        try {
            log_file = new FileWriter(args[0]+".log");
            log_file.append("NUMBER OF RECORDS RECIEVED: "+num_recieved+"\n");
            log_file.append("NUMBER OF SUCCESSES       : "+num_success+"\n");
            log_file.append("NUMBER OF FAILURES        : "+num_failure+"\n");
        } catch (IOException ex) {
            Logger.getLogger(DatabaseConverter.class.getName()).log(Level.SEVERE, null, ex);
        }

        
    }
    
}
