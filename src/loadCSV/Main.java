package loadCSV;

import java.io.*;
import java.util.*;
import java.lang.String;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;

public class Main {

	// varaibles to hold clustercfg data
		static String catalogDriver = "";
		static String catalogHostName = "";
		static String catalogUserName = "";
		static String catalogPassword = "";
		static ArrayList<String> sqlAL = new ArrayList<String>();
		static ArrayList<DBNode> nodeAL = new ArrayList<DBNode>();
		static String command = "";
		static int partition = -1;
		static String table = "";
		static String partitionCol = "";
		static String partitionParam1 = "";

		static int numnodes = -1;

		
		/* Main method
		 * Parameter: (String[]) args : contains filename. First slot is the
		 * 								clustercfg, second slot is the ddlfile
		 * 
		 * Description: takes two file names, clustercfg which contains the
		 * 				information on cluster nodes and csv file that contains
		 * 				the data to be read. Main method will call functions to
		 * 				read informations on the files and write the necessary
		 * 				informations. 
		 * 
		 * Returns: void
		*/
		public static void main(String[] args) throws InterruptedException {
			
			String clustercfg = "";
			String csvfile = "";
			String line = null;
			String sql = "";
			CsvReader csvRead = new CsvReader();
			ArrayList<String[]> csvData = null;
			boolean successful = false;
			
			FileReader fr;
			BufferedReader br;

			// int numnodes = 0;

			if (args.length > 1) {
				clustercfg = args[0];
				csvfile = args[1];

				try {
					fr = new FileReader(clustercfg);
					br = new BufferedReader(fr);
					int nodeNum = 1;
					int temp = 0;
					String param1 = null;
					String param2 = null;

					while ((line = br.readLine()) != null) {
						//System.out.println(line);
						
						if (numnodes == -1) {
							processCatalogLine(line);
						} else {
							String[] parts = line.split("=");
							
							if(parts[0].equals("partition.node" + nodeNum +".param1")) {
								param1 = parts[1];
								temp++;
							}
							else if(parts[0].equals("partition.node" + nodeNum +".param2")) {
								param2 = parts[1];
								temp++;
							}
							
							if (temp == 2) {
								DBNode newNode = new DBNode(null, null, null, null, param1, param2);
								nodeAL.add(newNode);
								temp = 0;
								nodeNum++;
								param1 = null;
								param2 = null;
							}
							//System.out.println(line);
							
							
						}

					} // end of while
					

					csvData = csvRead.readCSV(csvfile);
					readCatalog(table);
					cleanNodeAL();
					successful = runCSV(csvData);

					if (successful) {
						updateCatalog();
					} else {
						System.out.println("Error during sql query, Catalog not updated");
					}

				} catch (IOException e) {
					System.out.println("File not found");
				}

			} else {
				System.out.println("Argument insufficient");
			}

		}// end of main

		
		/* Function processCatalogLine
		 * Parameter: (String) line : line to be processed
		 * 
		 * Description: takes a line of String read from clustercfg.
		 * 				It takes the necessary information and passes the
		 * 				catalog values to global variables so the program
		 * 				can run updates to the catalog DB
		 * 
		 * Returns: void
		*/
		private static void processCatalogLine(String line) {

			String wordLeft = ""; // left side of catalog eqs
			String temp = "";
			/*
			 * switch for scanning catalogs. -1 = nothing 0 = driver, 1=hostname,
			 * 2=username, 3=passwd
			 *
			 */
			int catalog = -1;

			for (int x = 0; x < line.length(); x++) {
				// System.out.print(line.charAt(x));
				// catalog = compare(wordLeft);

				if (catalog == -1) {
					wordLeft = wordLeft + line.charAt(x);
					catalog = catalogCompare(wordLeft);
				} else if (catalog == 0) {
					catalogDriver = catalogDriver + line.charAt(x);
				} else if (catalog == 1) {
					catalogHostName = catalogHostName + line.charAt(x);
				} else if (catalog == 2) {
					catalogUserName = catalogUserName + line.charAt(x);
				} else if (catalog == 3) {
					catalogPassword = catalogPassword + line.charAt(x);
				} else if (catalog == 4) {
					numnodes = Integer.parseInt(line.substring(x));
					x = line.length() + 1;
				} else if (catalog == 5) {
					table = table + line.charAt(x);
				} else if (catalog == 6) {
					//partition = partition + line.charAt(x);
					temp = line.substring(x);

					if(temp.equals("range")) {
						partition = 1;
					} else if (temp.equals("hash")) {
						partition = 2;
					} else {
						partition = 0;
					}
					
					x = line.length() + 1; //terminates the loop
				} else if (catalog == 7) {
					partitionCol = partitionCol + line.charAt(x);
				} else if (catalog == 8) {
					partitionParam1 = partitionParam1 + line.charAt(x);
				} 

			}

		} // end of method

		/* Function catalogCompare 
		 * Parameter: (String) line : String to be processed
		 * 
		 * Description: Takes a string and compares it to a set of strings and
		 * 				returns a integer value based on the matches. 
		 * 
		 * Returns: (int) : if 0: the processCatalogLine function will look	for a catalog driver
		 * 					if 1: the processCatalogLine function will look for a catalog hostname						
		 * 					if 2: the processCatalogLine function will look for a catalog username	
		 * 					if 3: the processCatalogLine function will look for a catalog password	
		 * 					if 4: the processCatalogLine function will look for a numnode
		 * 					if 5: the processCatalogLine function will look for a tablename
		 *  				if 6: the processCatalogLine function will look for a partition method	
		 * 					if 7: the processCatalogLine function will look for a partition column
		 *  				if 8: the processCatalogLine function will look for a partition 1 variable 	
		 * 
		*/
		private static int catalogCompare(String line) {

			int ret = -1;

			if (line.equals("catalog.driver=")) {
				ret = 0;
			} else if (line.equals("catalog.hostname=")) {
				ret = 1;
			} else if (line.equals("catalog.username=")) {
				ret = 2;
			} else if (line.equals("catalog.passwd=")) {
				ret = 3;
			} else if (line.equals("numnodes=")) {
				ret = 4;
			} else if (line.equals("tablename=")) {
				ret = 5;
			} else if (line.equals("partition.method=")) {
				ret = 6;
			} else if (line.equals("partition.column=")) {
				ret = 7;
			} else if (line.equals("partition.param1=")) {
				ret = 8;
			}


			return (ret);
		}

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
		private static void runSQL(String JDBC_DRIVER, String DB_URL, String USER, String PASS, String sql) {

			Connection conn = null;
			Statement stmt = null;
			int column = 0;
			String val = "";
			
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
				} finally {
					
				}
				
				//stmt.executeUpdate(sql); //for ddl commands
				//stmt.executeQuery(sql); //for insert and what not
				
				if (rs != null) {
					// STEP 5: Extract data from result set
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

				System.out.println("[" + DB_URL.substring(0, (DB_URL.length() - 1)) + "]: sql success");

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

		}

		
		/* Function readCatalog
		 * Parameter: none
		 * 
		 * Description: attempts to establish a database connection to the catalog DB
		 * 				and get node data from Catalog. It will then insert data into
		 * 				nodeAL ArrayList
		 * 
		 * Returns: void
		*/
		private static void readCatalog(String table) {
			// runDDL(String JDBC_DRIVER, String DB_URL, String USER, String PASS,
			// String sql)

			//System.out.println(sqlAL.get(0));

			/*
			 * static String catalogDriver = ""; static String catalogHostName = "";
			 * static String catalogUserName = ""; static String catalogPassword =
			 * "";
			 */
			Connection conn = null;
			Statement stmt = null;
			String sql = "";
			
			//SELECT * FROM dtables WHERE tname ='' ORDER BY nodeid 
			
			String tname = "";
			String cmd = "";
			
			String nodeDriver = "";
			String nodeHostName = "";
			String nodeUserName = "";
			String nodePassword = "";
			int nodeid = 0;
			
			numnodes = 0;
			

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

				
				sql = "SELECT * FROM dtables WHERE tname='" + table+ "' ORDER BY nodeid ";
				//"SELECT * FROM dtables WHERE tname=??? ORDER BY nodeid"
				//System.out.println(sql);
				
				// STEP 5: Extract data from result set
				ResultSet rs = stmt.executeQuery(sql);
				
				System.out.println("Query executed");
				
				// STEP 5: Extract data from result set
				while (rs.next()) {
					
					// Retrieve by column name
					
					nodeDriver = rs.getString("nodedriver");
					nodeHostName = rs.getString("nodeurl");
					nodeUserName = rs.getString("nodeuser");
					nodePassword = rs.getString("nodepasswd");
					nodeid = rs.getInt("nodeid");
					
					nodeDriver = nodeDriver.trim().replaceAll(" +", "");
					nodeHostName = nodeHostName.trim().replaceAll(" +", "");
					nodeUserName = nodeUserName.trim().replaceAll(" +", "");
					nodePassword = nodePassword.trim().replaceAll(" +", "");
					
					if(nodeUserName.equals("")) {
						nodeUserName = " ";
					}
					if(nodePassword.equals("")) {
						nodePassword = " ";
					}	
					
					// Display values
					/*
					System.out.print("Driver: " + nodeDriver);
					System.out.print(", URL: " + nodeHostName);
					System.out.print(", User: " + nodeUserName);
					System.out.println(", Pass: " + nodePassword);
					*/
					
					
					if (nodeAL.get(numnodes) == null) {
						DBNode newNode = new DBNode(nodeDriver, nodeHostName, nodeUserName, nodePassword);
						nodeAL.add(numnodes, newNode);
					} else {
						nodeAL.get(numnodes).setDriver(nodeDriver);
						nodeAL.get(numnodes).setHostname(nodeHostName);
						nodeAL.get(numnodes).setUsername(nodeUserName);
						nodeAL.get(numnodes).setPassword(nodePassword);
					}
					
					numnodes++;
					
				}
				// STEP 6: Clean-up environment
				 
				 // Display values //System.out.print("ID: " + id);
				 //System.out.println(", name: " + name); }

				rs.close();
				stmt.close();
				conn.close();
				//System.out.println("[" + catalogHostName.substring(0, (catalogHostName.length() - 1)) + "]: Catalog Updated");
			} catch (SQLException se) {
				// Handle errors for JDBC
				// se.printStackTrace();
				System.out.println(
						"[" + catalogHostName.substring(0, (catalogHostName.length() - 1)) + "]: catalog read failed");
			} catch (Exception e) {
				// Handle errors for Class.forName
				// e.printStackTrace();
				System.out.println(
						"[" + catalogHostName.substring(0, (catalogHostName.length() - 1)) + "]: catalog read failed");
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
		
		/* Function updateCatalog
		 * Parameter: none
		 * 
		 * Description: attempts to establish a database connection to the catalog DB
		 * 				and run update to metadata table. Will close connection after 
		 * 				update is completed
		 * 
		 * Returns: void
		*/
		private static void updateCatalog() {


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
						sql = catalogMakeSql(nodeAL.get(x), x+1);
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
		private static String catalogMakeSql(DBNode node, int nodeid) {
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
		
		/* Function cleanNodeAL
		 * Parameter: none
		 * 
		 * Description: Removes nodes in nodeAL that has no driver info. This is
		 * 				to weed out nodes that has incomplete information.
		 * 
		 * Returns: void
		*/
		private static void cleanNodeAL() {
			
			for(int x = 0; x < nodeAL.size();x++) {
				if(nodeAL.get(x).getDriver() == null) {
					nodeAL.remove(x);
					x--;
				}
			}
		}
		
		
		/* Function runCSV
		 * 
		 * Parameter: (ArrayList<String[]>) ArrayList containing csvdata
		 * 
		 * Description: Takes an arrayList and depending on the partition memthod,
		 * 				calls on one of the runCSVx methods
		 * 
		 * Returns: (boolean) success: if true, CSV data is successfully written to nodes
		*/
		private static boolean runCSV(ArrayList<String[]> csvData) {
			boolean success = false;
			
			if (partition == 1) { //range
				runCSVRange(csvData);
				success = true;
			} else if (partition == 2){ //hash
				runCSVHash(csvData);
				success = true;
			} else {
				runCSVnoPartition(csvData);
				success = true;
			}
				
			
			return(success);
		}
		
		
		/* Function runCSVnoPartition
		 * 
		 * Parameter: (ArrayList<String[]>) ArrayList containing csvdata
		 * 
		 * Description: Takes csvdata and writes to everysingle database node
		 * 
		 * Returns: void
		*/
		private static void runCSVnoPartition(ArrayList<String[]> csvData) {
			//ResultSetMetaData metaData = getMetaData();

			int column = getMetaData();
			String sql = "";
			
			System.out.println(column);
			
			for (int x = 0; x < csvData.size();x++) {
				
				sql = csvSQL(csvData.get(x));
				//System.out.println(sql);
				
				for(int y = 0; y < nodeAL.size();y++) {
					runSQL(nodeAL.get(y).getDriver(), nodeAL.get(y).getHostname(), nodeAL.get(y).getUsername(), nodeAL.get(y).getPassword(), sql);
				} 
			}
				
		}
		
		
		/* Function runCSVRange
		 * 
		 * Parameter: (ArrayList<String[]>) ArrayList containing csvdata
		 * 
		 * Description: Takes csvdata and writes to database nodes that
		 * 				fits the range.
		 * 
		 * Returns: void
		*/
		private static void runCSVRange(ArrayList<String[]> csvData) {
			//ResultSetMetaData metaData = getMetaData();

			int column = getMetaData();
			String sql = "";
			
			System.out.println(column);
			
			for (int x = 0; x < csvData.size();x++) {
				
				sql = csvSQL(csvData.get(x));
				//System.out.println(sql);
				
				
				for(int y = 0; y < nodeAL.size();y++) {
					int colval = Integer.parseInt(csvData.get(x)[column]);
						
					if (nodeAL.get(y).getPartitionParam1().equals("-inf")) {
							
						if (nodeAL.get(y).getPartitionParam2().equals("+inf")) {
							runSQL(nodeAL.get(y).getDriver(), nodeAL.get(y).getHostname(), nodeAL.get(y).getUsername(), nodeAL.get(y).getPassword(), sql);
						} else if(colval <= Integer.parseInt(nodeAL.get(y).getPartitionParam2())) {
							runSQL(nodeAL.get(y).getDriver(), nodeAL.get(y).getHostname(), nodeAL.get(y).getUsername(), nodeAL.get(y).getPassword(), sql);
						}
					} else if (nodeAL.get(y).getPartitionParam2().equals("+inf")) {
						if (Integer.parseInt(nodeAL.get(y).getPartitionParam1()) < colval) {
							runSQL(nodeAL.get(y).getDriver(), nodeAL.get(y).getHostname(), nodeAL.get(y).getUsername(), nodeAL.get(y).getPassword(), sql);
						}
					} else {
						int p1 = Integer.parseInt(nodeAL.get(y).getPartitionParam1());
						int p2 =  Integer.parseInt(nodeAL.get(y).getPartitionParam2());
							
						if(p1 < colval && colval <= p2) {
							runSQL(nodeAL.get(y).getDriver(), nodeAL.get(y).getHostname(), nodeAL.get(y).getUsername(), nodeAL.get(y).getPassword(), sql);
						}
							
					}
						
				} 
			}
				
		}
				
			
			
		/* Function runCSVHash
		 * 
		 * Parameter: (ArrayList<String[]>) ArrayList containing csvdata
		 * 
		 * Description: Takes csvdata and writes to database nodes based
		 * 				on the hashing alhorithm.
		 * 
		 * NOTE: Because of how I implemented this program, there is no need
		 * to add 1 for the hash value
		 * 
		 * Returns: void
		*/
		private static void runCSVHash(ArrayList<String[]> csvData) {
			//ResultSetMetaData metaData = getMetaData();

			int column = getMetaData();
			int p1 = 0;
			int i = 0;
			String sql = "";
				
			String dr = "";
			String hn = "";
			String un = "";
			String pw = "";
				
			System.out.println(column);
				
			for (int x = 0; x < csvData.size();x++) {
					
				sql = csvSQL(csvData.get(x));
				p1 = Integer.parseInt(csvData.get(x)[column]);
				i = ( p1 % nodeAL.size());
					
				dr = nodeAL.get(i).getDriver();
				hn = nodeAL.get(i).getHostname();
				un = nodeAL.get(i).getUsername();
				pw = nodeAL.get(i).getPassword();
					
				runSQL(dr, hn, un, pw, sql);
					
			}

		}
		
		/* Function csvSQL
		 * Parameter: (String[]) csvData : Array that holds csvdata
		 * 
		 * Description: Takes a string array and outputs an sql that will
		 * 				write the csv data if executed
		 * 
		 * Returns: (String) data : sql query to write into db nodes
		*/
		private static String csvSQL(String[] csvData) {
			String sql = "INSERT INTO " + table + " VALUES (";
			String data = "";
			
			for (int x = 0; x < csvData.length; x++) {
				data = csvData[x];
				
				if (data.charAt(0) == '"' && data.charAt(data.length() - 1) == '"') { //replaces double quote with single quote
					data = "'" + data.substring(1, data.length() -1)  + "'";
				}
				
				if(x > 0) {
					sql = sql + ",";
				}
				
				sql = sql + data;
			}
			sql = sql + ")";
			
			return(sql);
		}
		
		/* Function getMetaData
		 * Parameter: none
		 * 
		 * Description: attempts to return the index of the partition column
		 * 
		 * Returns: (int) ret: The column of the partition
		*/
		private static int getMetaData() {
			ResultSetMetaData rsmd = null;
			int ret = -1;
			
			String JDBC_DRIVER = nodeAL.get(0).getDriver();
			String DB_URL = nodeAL.get(0).getHostname();
			String USER = nodeAL.get(0).getUsername();
			String PASS = nodeAL.get(0).getPassword();
			Connection conn = null;
			Statement stmt = null;
			int column = -1;
			String val = "";
			String sql = "SELECT * FROM " + table;
			
			try {
				// STEP 2: Register JDBC driver
				Class.forName(JDBC_DRIVER);

				// STEP 3: Open a connection

				if (USER.equals(" ") && PASS.equals(" ")) {
					conn = DriverManager.getConnection(DB_URL);
				} else {
					conn = DriverManager.getConnection(DB_URL, USER, PASS);
				}
				
				// STEP 4: Execute a query
				stmt = conn.createStatement();

				ResultSet rs = null;
				
				rs = stmt.executeQuery(sql);
				
				if (rs != null) {
					rsmd = rs.getMetaData();
					
					
					for (int x = 1; x <= rsmd.getColumnCount();x++) {
						if (partitionCol.equalsIgnoreCase(rsmd.getColumnLabel(x))){
							ret = x - 1;
						}
						
						//System.out.println(rsmd.getColumnTypeName(x));
						//System.out.println(rsmd.getColumnLabel(x));
					}
					
					
					rs.close();
				}

				//System.out.println("[" + DB_URL.substring(0, (DB_URL.length() - 1)) + "]: sql success");

				stmt.close();
				conn.close();
				
			} catch (SQLException se) {
				// Handle errors for JDBC
				// se.printStackTrace();
				System.out.println("[" + DB_URL + "]: sql metadata failed");
			} catch (Exception e) {
				// Handle errors for Class.forName
				// e.printStackTrace();
				System.out.println("[" + DB_URL + "]: sql metadata failed");
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
		
		/* Function getCmd
		 * Parameter: (String) sql : sql query to extract information from
		 * 
		 * Description: gets the ddl command from a sql query.
		 * 
		 * Returns: (String) : The ddl command of the sql in regards to table, 
		 * 						such as create or drop.
		*/
		public static String getCmd(String sql) {
			String ret = "";

			char ch;
			String word = "";

			for (int x = 0; x < sql.length(); x++) {
				ch = sql.charAt(x);
				// word = word + ch;

				if (ch == ' ') {
					x = sql.length() + 1;
				} else {
					word = word + ch;
				}
			}

			ret = word;

			return (ret);
		}
	
}
