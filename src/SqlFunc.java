

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlFunc {

	/* Function runSQL
	 * Parameter: (String) JDBC_DRIVER : driver for the database node
	 * 				(String) DB_URL : url/hostname for the database node
	 * 				(String) USER : username for the database node
	 * 				(String) PASS : password for the database node
	 * 				(String) sql : sql to be executed
	 * 
	 * Description: attempts to establish a datbase connection and run a
	 * 				sql query. Will close connection after query is completed
	 * 
	 * Returns: void
	*/
	public static boolean runJoin(DBNode node1, DBNode node2, String sql) {

		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		int column = 0;
		String val = "";
		String tempTable = node1.getTable() + "_temp";
		boolean ret = false;
		String datatype = "";
		String col = "";
		String sqlTemp = "";
		String result = "";
		
		try {
			// STEP 2: Register JDBC driver
			System.out.println("Loading driver...");
			Class.forName(node1.getDriver());

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");

			if (node1.getUsername().equals(" ") && node1.getPassword().equals(" ")) {
				conn1 = DriverManager.getConnection(node1.getHostname());
			} else {
				conn1 = DriverManager.getConnection(node1.getHostname(), node1.getUsername(), node1.getPassword());
			}
			
			if (node2.getUsername().equals(" ") && node2.getPassword().equals(" ")) {
				conn2 = DriverManager.getConnection(node2.getHostname());
			} else {
				conn2 = DriverManager.getConnection(node2.getHostname(), node2.getUsername(), node2.getPassword());
			}
			
			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt1 = conn1.createStatement();
			stmt2 = conn2.createStatement();

			ResultSet rs = stmt1.executeQuery("select * from " + node1.getTable()); //REMOVE PLACEHOLDER VALUE
			ResultSetMetaData rsmd = rs.getMetaData();
			
			ResultSet rs2 = stmt2.executeQuery("select * from " + node2.getTable()); //REMOVE PLACEHOLDER VALUE!
			ResultSetMetaData rsmd2 = rs2.getMetaData();
			column = rsmd.getColumnCount();
			
			
			// STEP 5: Extract data from result set
			sqlTemp = "CREATE TEMPORARY TABLE " + tempTable + "(";

			for (int i = 1; i <= column; i++) {
				datatype = rsmd.getColumnTypeName(i);
				col = rsmd.getColumnName(i);
				
				if (i > 1) {
					sqlTemp = sqlTemp + ", ";
				}
				
				if(datatype.equals("CHAR")) {
					sqlTemp = sqlTemp + col + " " + "CHAR(255)";
				} else if (datatype.equals("VARCHAR")) {
					sqlTemp = sqlTemp + col + " " + "VARCHAR(255)";
				} else {
					sqlTemp = sqlTemp + col + " " +  datatype;
				}
				
			}
			sqlTemp = sqlTemp + ")";
			//System.out.println(sql);
			stmt2.executeUpdate(sqlTemp);
			
			sqlTemp = "INSERT INTO " + tempTable + " VALUES ";
			while(rs.next()){
				sqlTemp = sqlTemp + "(";
				for (int i = 1; i <= column; i++) {
					
					datatype = rsmd.getColumnTypeName(i);
					col = rsmd.getColumnName(i);
					
					//System.out.print("[" + col + "]");
					//System.out.print("(" + datatype + ")");
					
					if(datatype.equals("CHAR") || datatype.equals("VARCHAR")) {
						sqlTemp = sqlTemp + "'" + rs.getString(i) + "', ";
					} else {
						sqlTemp = sqlTemp + rs.getString(i) + ", ";
					}
					/*
					if (i > 1) {
						System.out.print(", ");
					}
					*/
					val = rs.getString(i);
					//System.out.print(val);
					
				}
				sqlTemp = sqlTemp.substring(0, sqlTemp.length() - 2);
				sqlTemp = sqlTemp + "), ";
		      }
			sqlTemp = sqlTemp.substring(0, sqlTemp.length() - 2);
			//System.out.println(sql);
			stmt2.executeUpdate(sqlTemp);
			
			//sql = "select * from " + tempTable + ", " + node2.getTable() + " WHERE " + tempTable + ".id=" + node2.getTable() + ".id";
			//sql = "select * from " + tempTable + ", " + node2.getTable();
			//sql = sql.replaceAll("\\b" + node1.getTable() +"\\b", tempTable);
			sql = sql.replaceFirst(node1.getTable(), tempTable);
			System.out.println(sql);
			rs2 = stmt2.executeQuery(sql);
			rsmd2 = rs2.getMetaData();
			column = rsmd2.getColumnCount();
			
			
			while(rs2.next()){
				
				/*
				for (int i = 1; i <= column; i++) {
					System.out.print(rsmd.getColumnName(i));
				}
				*/
				
				
				for (int i = 1; i <= column; i++) {
					if (i > 1) {
						result = result + ", ";
						//System.out.print(", ");
					}
					val = rs2.getString(i);
					result = result + val;
					//System.out.print(val);
				}
				System.out.println(result);
				result = "";
				//System.out.println();

		      }
			
			//sql = "DROP TABLE " + tempTable;
			//stmt2.executeUpdate(sql);
			

			System.out.println("[Join Operation Success!]");
			ret = true;

			rs.close();
			rs2.close();
			stmt1.close();
			conn1.close();
			stmt2.close();
			conn2.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			// se.printStackTrace();
			System.out.println("[Join Operation Failed]");
		} catch (Exception e) {
			// Handle errors for Class.forName
			// e.printStackTrace();
			System.out.println("[Join Operation Failed]");
		} finally {
			// finally block used to close resources
			try {
				if (stmt1 != null)
					stmt1.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (stmt2 != null)
					stmt2.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn1 != null)
					conn1.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
			try {
				if (conn2 != null)
					conn2.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
			// System.out.println("Goodbye!");

		return(ret);
	}
	
	/* Function runDDL
	 * Parameter: (String) JDBC_DRIVER : driver for the database node
	 * 				(String) DB_URL : url/hostname for the database node
	 * 				(String) USER : username for the database node
	 * 				(String) PASS : password for the database node
	 * 				(String) sql : sql to be executed
	 * 
	 * Description: attempts to establish a datbase connection and run a
	 * 				sql query. Will close connection after query is completed
	 * 
	 * Returns: void
	*/
	public static boolean runSQL(String JDBC_DRIVER, String DB_URL, String USER, String PASS, String sql) {

		Connection conn = null;
		Statement stmt = null;
		int column = 0;
		String val = "";
		boolean ret = false;
		
		
		try {
			// STEP 2: Register JDBC driver
			System.out.println("Loading driver...");
			Class.forName(JDBC_DRIVER);

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");

			if (USER.equals(" ") && PASS.equals(" ")) {
				conn = DriverManager.getConnection(DB_URL);
			} else {
				conn = DriverManager.getConnection(DB_URL, USER, PASS);
			}
			
			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			
			ResultSet rs = null;
			
			try {
				rs = stmt.executeQuery(sql);
			} catch (SQLException se) {
				stmt.executeUpdate(sql);
			}
			
			if (rs != null) {
				ResultSetMetaData rsmd = rs.getMetaData();
				column = rsmd.getColumnCount();
				
				while(rs.next()){
					
					/*
					for (int i = 1; i <= column; i++) {
						System.out.print(rsmd.getColumnName(i));
					}
					*/
					
					
					for (int i = 1; i <= column; i++) {
						if (i > 1) {
							System.out.print(", ");
						}
						val = rs.getString(i);
						System.out.print(val);
					}
					System.out.println();

			      }
				
				rs.close();
			}
			
			//stmt.executeUpdate(sql); //for ddl commands
			//stmt.executeQuery(sql); //for insert and what not
			
			// STEP 5: Extract data from result set

			System.out.println("[" + DB_URL + "]: sql success");
			ret = true;

			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			// se.printStackTrace();
			System.out.println("[" + DB_URL + "]: sql failed");
		} catch (Exception e) {
			// Handle errors for Class.forName
			// e.printStackTrace();
			System.out.println("[" + DB_URL + "]: sql failed");
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

		return(ret);
	}
	
	/* Function runDDL
	 * Parameter: (String) JDBC_DRIVER : driver for the database node
	 * 				(String) DB_URL : url/hostname for the database node
	 * 				(String) USER : username for the database node
	 * 				(String) PASS : password for the database node
	 * 				(String) sql : sql to be executed
	 * 
	 * Description: attempts to establish a datbase connection and run a
	 * 				sql query. Will close connection after query is completed
	 * 
	 * Returns: void
	*/
	public static boolean runDDL(String JDBC_DRIVER, String DB_URL, String USER, String PASS, String sql) {
		boolean successful = false;
		Connection conn = null;
		Statement stmt = null;
		try {
			// STEP 2: Register JDBC driver
			System.out.println("Loading driver...");
			Class.forName(JDBC_DRIVER);

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			
			if (USER.equals(" ") && PASS.equals(" ")) {
				conn = DriverManager.getConnection(DB_URL);
			} else {
				conn = DriverManager.getConnection(DB_URL, USER, PASS);
			}

			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt = conn.createStatement();

			// ResultSet rs = null;
			stmt.executeUpdate(sql);
			// stmt.executeQuery(sql);

			// STEP 5: Extract data from result set

			/*
			 * while (rs.next()) { // Retrieve by column name //int id =
			 * rs.getInt("id"); //String name = rs.getString("name");
			 * 
			 * // Display values //System.out.print("ID: " + id);
			 * //System.out.println(", name: " + name); }
			 */

			System.out.println("[" + DB_URL + "]: sql success");
			successful = true;

			// rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			//se.printStackTrace();
			System.out.println("[" + DB_URL + "]: sql failed");
		} catch (Exception e) {
			// Handle errors for Class.forName
			//e.printStackTrace();
			System.out.println("[" + DB_URL + "]: sql failed");
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
		return(successful);
	}
	
}
