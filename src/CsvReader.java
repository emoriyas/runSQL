import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.opencsv.CSVReader;

public class CsvReader {

	public CsvReader(){
	}
	
	//p
	public ArrayList<String[]> readCSV(String file){
		
	    String [] nextLine;
		ArrayList<String[]> csv = new ArrayList<String[]>();
		
		try {
			CSVReader reader = new CSVReader(new FileReader(file), ',', '\0');
			
		     while ((nextLine = reader.readNext()) != null) {
		    	 csv.add(nextLine);
			 }
		     
		     reader.close();
			
		} catch(FileNotFoundException e) {
			System.out.println("CSV file not found");
			csv = null;
		} catch(IOException e) {
			System.out.println("error while reading csv");
			csv = null;
		}
		
		
		return(csv);
	}
	
	public void printCSV(ArrayList<String[]> csv){
	     for(int i = 0; i < csv.size();i++) {
	    	 for(int j = 0; j < csv.get(i).length; j++) {
	    		 if (j > 0) {
	    			 System.out.print(", ");
	    		 }
	    		 System.out.print(csv.get(i)[j]);
	    	 }
	    	 System.out.println();
	     }
	}
	
	/* Function updateCatalog
	 * Parameter: none
	 * 
	 * Description: attempts to establish a database connection to the catalog DB
	 * 				and run update to metadata table. Will close connection after 
	 * 				update is completed
	 * 
	 * Returns: void
	*/
	public static void updateCatalog(DBNode catalog, ArrayList<DBNode> nodeAL, int partition, String table, String partitionCol) {

		String catalogDriver = catalog.getDriver();
		String catalogHostName = catalog.getHostname();
		String catalogUserName = catalog.getUsername();
		String catalogPassword = catalog.getPassword();

		Connection conn = null;
		Statement stmt = null;
		String sql = "";

		try {
			// STEP 2: Register JDBC driver
			Class.forName(catalogDriver);

			// STEP 3: Open a connection
			if (catalogUserName.equals(" ") && catalogPassword.equals(" ")) {
				conn = DriverManager.getConnection(catalogHostName);
			} else {
				conn = DriverManager.getConnection(catalogHostName, catalogUserName, catalogPassword);
			}

			// STEP 4: Execute a query
			stmt = conn.createStatement();

			for (int x = 0; x < nodeAL.size(); x++) {

				if (nodeAL.get(x).getDriver() != null) {
					sql = catalogMakeSql(nodeAL.get(x), x+1, partition, table, partitionCol);
					stmt.executeUpdate(sql);
				}

			}

			// rs.close();
			stmt.close();
			conn.close();
			System.out
					.println("[" + catalogHostName.substring(0, (catalogHostName.length() - 1)) + "]: Catalog Updated");
		} catch (SQLException se) {
			// Handle errors for JDBC
			// se.printStackTrace();
			System.out.println(
					"[" + catalogHostName.substring(0, (catalogHostName.length() - 1)) + "]: catalog update failed");
		} catch (Exception e) {
			// Handle errors for Class.forName
			// e.printStackTrace();
			System.out.println(
					"[" + catalogHostName.substring(0, (catalogHostName.length() - 1)) + "]: catalog update failed");
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
			// System.out.println("Goodbye!");

	}
	
	/* Function catalogMakeSql
	 * Parameter: none
	 * 
	 * Description: Depending on the partition method, creates a sql query to
	 * 				update the catalog metadata
	 * 
	 * Returns: (String) sql : sql to be used by updateCatalog()
	*/
	private static String catalogMakeSql(DBNode node, int nodeid, int partition, String table, String partitionCol) {
		String sql = "";
		
		if (partition == 1) { //range
			sql = "UPDATE dtables "
					+ "SET partmtd=1,partcol='" + partitionCol + "',partparam1='" + node.getPartitionParam1() + "',"
					+ "partparam2='" + node.getPartitionParam2() + "' "
					+ "WHERE tname='" + table + "' "
					+ "AND nodeid=" + nodeid + " "
					+ "AND nodedriver='" + node.getDriver() + "' "
					+ "AND nodeurl='" + node.getHostname() + "' "
					+ "AND nodeuser='" + node.getUsername() + "' "
					+ "AND nodepasswd='" + node.getPassword() + "'";
		} else if (partition == 2){ //hash
			sql = "UPDATE dtables "
					+ "SET partmtd=2,partcol='" + partitionCol + "'"
					+ "WHERE tname='" + table + "' "
					+ "AND nodeid=" + nodeid + " "
					+ "AND nodedriver='" + node.getDriver() + "' "
					+ "AND nodeurl='" + node.getHostname() + "' "
					+ "AND nodeuser='" + node.getUsername() + "' "
					+ "AND nodepasswd='" + node.getPassword() + "'";
		} else {
			sql = "UPDATE dtables "
					+ "SET partmtd=0  "
					+ "WHERE tname='" + table + "' "
					+ "AND nodeid=" + nodeid + " "
					+ "AND nodedriver='" + node.getDriver() + "' "
					+ "AND nodeurl='" + node.getHostname() + "' "
					+ "AND nodeuser='" + node.getUsername() + "' "
					+ "AND nodepasswd='" + node.getPassword() + "'";
		}
			
		
		return(sql);
	}
	
}
