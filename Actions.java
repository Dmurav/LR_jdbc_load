/*
 * LoadRunner Java script. (Build: _01_)
 * 
 * Script Description: 
 *                     
 */

import lrapi.lr;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;
import java.util.Random;
import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileWriter;

public class Actions

{
	// Create global connection variable
    private Connection connection;

    // VUser Init. Get connection with DB
    public int init() throws ClassNotFoundException, SQLException {
        // Initialize DB connection
        try {
	    	// Load JDBC Driver
	    	String jdbc_driver = lr.eval_string("{jdbcdriver}");
            Class.forName(jdbc_driver);
        } catch (Exception ex) {
	    	// If driver load is unsuccessful
	    	lr.log_message("Database Driver not found");
	    	lr.abort();
		}
        
		try {
	    	// Specify the JDBC Connection String
	    	String url = lr.eval_string("{url}");
	    	// Connect to URL using USERNAME and PASSWORD
	    	String username = lr.eval_string("{username}");
	    	String password = ""; //lr.eval_string("{password}");
	    	connection = DriverManager.getConnection(url, username, password);
	    	lr.log_message("JDBC Connection Successful");
     	} catch (SQLException e) {
	    	// If Connection Failed
	    	lr.log_message("Database Connection Failed, Please check your connection string");
	    	lr.abort();
		}
	    	return 0;
    } //end of init

    // VUser action
    public int action() throws ClassNotFoundException, SQLException {
    	

    	int numThread = lr.eval_int("{currentThread}");
		int numIteration = lr.eval_int("{namIteration}");
		int randCondition = getRandomNum(1, 4);
    	int randUpdateNum = getRandomNum(1, numThread + 1);
		String ranStr = getRandomString();
		
    	if (randCondition == 1){
    		// Database SELECT Query
			lr.start_transaction("Database_Query_SELECT");
			database_query("select ID, DATE from LOADRUNNER where ITERATION =" + (numIteration - 1));
			lr.end_transaction("Database_Query_SELECT", lr.AUTO);
    	
    	} else if (randCondition == 2){
			// Database INSERT Query
			lr.start_transaction("Database_Query_INSERT");
			String query = "insert into LOADRUNNER values (null, current_timestamp," + "'" +  ranStr + "'" + "," + numThread + "," + numIteration + ")";
			database_update(query);
			lr.end_transaction("Database_Query_INSERT", lr.AUTO);
    		
    	} else if (randCondition == 3){
    		// Database UPDATE Query
			lr.start_transaction("Database_Query_UPDATE");
    		String query = "update LOADRUNNER set name=" + "'" +  ranStr + "'" + ", thread=" + numThread + ", iteration=" + numIteration  + " where thread =" + randUpdateNum;
    		database_update(query);
			lr.end_transaction("Database_Query_UPDATE", lr.AUTO);
    	}
		
	return 0;
    } //end of action

    // VUser end
    public int end() throws Throwable {
		
		connection = null;
        return 0;
    } //end of end

 
   //============= QUERY and other METHODS ===================================================================================================================================================================
    
    public int database_query(String SQL_QUERY) {
       
        Statement stmt;
        ResultSet rset;

        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rset = stmt.executeQuery(SQL_QUERY);

            if (rset != null) {
            	try{
            		lr.log_message(rset.getString(1) + "," + rset.getString(2));
            	} catch (Exception e){
            		lr.log_message("Nothing data to read and write to output :( ");
            	}
            }
            
            int size = 0;
            if (rset != null) {
                rset.last();
                size = rset.getRow();
            }
            rset.first();
            if (size % 3 == 0 && size != 0) {
            	String file = lr.eval_string("{output_file}");
                CSVWriter writer = new CSVWriter(new FileWriter(file));
                while (rset.next()) { 
                    try {
                        String[] toCSV = (rset.getString(1) + "," + rset.getString(2)).split(",");
                        writer.writeNext(toCSV);
                    } catch (Exception e){
                        lr.log_message("Write CSV Executed Not Successfully");
                    }
                }
                lr.log_message("Write CSV Executed Successfully");
                writer.close();
            }
            rset.close();
		} catch (Exception e) {
	    	lr.log_message("Caught Exception: " + e.getMessage());
	    	lr.set_transaction_status(lr.FAIL);
	    	return 1;
    	}
		return 0;
    }
    
    public int database_update(String SQL_QUERY) {
       	
    	Statement stmt = null;
       	
       	try {
    		
	   		connection.setAutoCommit(true);
	   		stmt = connection.createStatement();
	   		stmt.executeUpdate(SQL_QUERY);
	   		lr.set_transaction_status(lr.PASS);
	  		
		} catch (Exception e) {
	    	lr.log_message("Caught Exception: " + e.getMessage());
	    	lr.set_transaction_status(lr.FAIL);
	    	return 1;
    	}
		return 0;
    }
    
    // Function: generate random string
    public String getRandomString(){
       	int top = 6;
       	char data = ' ';
       	String rString = "";
       	Random ran = new Random();
    
       	for (int i=0; i<=top; i++){
           data = (char)(ran.nextInt(25)+97);
           rString = data + rString;
       	}
      	return rString;
    }
    
    // Function: generate random number
    public int getRandomNum(int low, int high){
    	Random r = new Random();
		return r.nextInt(high-low) + low;
    }
     
}