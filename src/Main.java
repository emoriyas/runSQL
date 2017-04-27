//package runDDL;

//import java.io.IOException;
//import java.io.FileReader;
//import java.io.BufferedReader;
import java.io.*;
import java.util.*;

//import loadCSV.CsvReader;

import java.lang.String;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Author: Eric Moriyasu ICS 421 Assignment 2
 * 
 * reads from clustercfg and sqlfile to run a multi threaded sql queries
 * 
 * Jan. 23 2017
 */


public class Main {

	// variables to hold clustercfg data
	//static String catalogDriver = "";
	//static String catalogHostName = "";
	//static String catalogUserName = "";
	//static String catalogPassword = "";
	static ArrayList<String> sqlAL = new ArrayList<String>();
	static ArrayList<DBNode> nodeAL1 = new ArrayList<DBNode>();
	static ArrayList<DBNode> nodeAL2 = new ArrayList<DBNode>();
	static boolean successful = false;
	static DBNode localNode = null;

	static String command = "";
	static int partition = -1;
	static String table = "";
	static String partitionCol = "";
	static String partitionParam1 = "";
	static int numnodes = -1;
	static boolean ddl = false;

	
	/* Main method
	 * Parameter: (String[]) args : contains filename. First slot is the
	 * 								clustercfg, second slot is the ddlfile
	 * 
	 * Description: takes two file names, clustercfg which contains the
	 * 				information on cluster nodes and ddlfile that contains
	 * 				the sql queries. Main method will call functions to
	 * 				read informations on the files and write the necessary
	 * 				informations. 
	 * 
	 * Returns: void
	*/
	public static void main(String[] args) throws InterruptedException {

		String clustercfg = "";
		String sqlfile = "";
		String line = null;
		String wordLeft = ""; // left side of catalog eqs
		String wordRight = ""; // right side of catalog eqs
		String[] tables = null;
		DBNode catalogNode = new DBNode();
		DBNode localNode = new DBNode();
		
		int nodeCount = 0;
		boolean csv = false;
		
		FileReader fr;
		BufferedReader br;

		// int numnodes = 0;

		if (args.length > 1) {
			clustercfg = args[0];
			sqlfile = args[1];

			try {
				fr = new FileReader(clustercfg);
				br = new BufferedReader(fr);
				int nodeNum = 1;
				int temp = 0;
				String nodeDriver = "";
				String nodeHostName = "";
				String nodeUserName = "";
				String nodePassword = "";
				String param1 = null;
				String param2 = null;

				while ((line = br.readLine()) != null) {
					//System.out.println(line);
					
					if(!table.equals("") && csv == false) {
						csv = true;
						tables = new String[] {table};
					}

					if (numnodes == -1) {
						processCatalogLine(line, catalogNode);
					} else {
						String[] parts = line.split("=");
						//System.out.println(line);
						//System.out.println(parts[0] + " = ");
						//System.out.println(parts[1]);
						
						if(parts.length > 1) {
							//System.out.println(parts[1]);
							//System.out.println(temp);
						}
						
						if(parts[0].equals("partition.node" + nodeNum +".param1")) {
							param1 = parts[1];
							temp++;
						} else if(parts[0].equals("partition.node" + nodeNum +".param2")) {
							param2 = parts[1];
							temp++;
						} else if(parts[0].equals("node" + nodeNum +".driver")) {
							nodeDriver = parts[1];
							temp++;
						} else if(parts[0].equals("node" + nodeNum +".hostname")) {
							nodeHostName = parts[1];
							temp++;
						} else if(parts[0].equals("node" + nodeNum +".username")) {
							nodeUserName = parts[1];
							temp++;
						} else if(parts[0].equals("node" + nodeNum +".passwd")) {
							nodePassword = parts[1];
							temp++;
						}
						
						if (temp == 2 && csv) {
							DBNode newNode = new DBNode(null, null, null, null, param1, param2);
							nodeAL1.add(newNode);
							temp = 0;
							nodeNum++;
							param1 = null;
							param2 = null;
						} else if (temp == 4 && !csv) {
							DBNode newNode = new DBNode(nodeDriver, nodeHostName, nodeUserName, nodePassword);
							nodeAL1.add(newNode);
							
							/*
							System.out.println("NODE " + (nodeCount + 1));
							System.out.println("NodeDriver: " + nodeDriver);
							System.out.println("NodeHostName: " + nodeHostName);
							System.out.println("NodeUserName: " + nodeUserName);
							System.out.println("NodePassword: " + nodePassword);
							*/
							nodeDriver = "";
							nodeHostName = "";
							nodeUserName = "";
							nodePassword = "";
							nodeNum++;
							nodeCount++;
							temp = 0;
						}
						//System.out.println(line);
					}
					
					
					
				} // end of while
				
				readSQL(sqlfile);
				
				if(!csv) {
					tables = SqlParse.getTable(sqlAL.get(0));
					ddl = SqlParse.isDDL(sqlAL.get(0));
				}
				
				if(!ddl) {
					System.out.println(tables);
					readCatalog(tables, catalogNode);
				}
				//doThread();
				
				//cmd = SqlParse.getCmd(sqlAL.get(0));
				
				if (csv) {
					ArrayList<String[]> csvData = null;
					CsvReader csvRead = new CsvReader();
					
					csvData = csvRead.readCSV(sqlfile);
					System.out.println("CSV");
					cleanNodeAL();
					successful = runCSV(csvData);
					
					if (successful) {
						//(DBNode catalog, ArrayList<DBNode> nodeAL, int partition, String table, String partitionCol)
						CsvReader.updateCatalog(catalogNode, nodeAL1, partition, table, partitionCol);
					} else {
						System.out.println("Error during sql query, Catalog not updated");
					}
				} else {
					doThread();
					//System.out.println("SQL");
				}
				
				
				//Sql.runJoin(nodeAL1.get(0), nodeAL2.get(0), "asdf");

				if (successful && ddl) {
					CatalogFunc.updateCatalogDDL(catalogNode, nodeAL1, sqlAL);
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
	private static void processCatalogLine(String line, DBNode catalogNode) {

		String wordLeft = ""; // left side of catalog eqs
		String[] words = line.split("=");
		String temp = "";
		
		/*
		 * switch for scanning catalogs. -1 = nothing 0 = driver, 1=hostname,
		 * 2=username, 3=passwd
		 *
		 */
		int catalog = -1;
		
		catalog = catalogCompare(words[0] + "=");
		
		if (catalog == 0) {
			catalogNode.setDriver(words[1]);
			//catalogNode.setDriver(catalogNode.getDriver() + line.charAt(x));
		} else if (catalog == 1) {
			catalogNode.setHostname(words[1]);
		} else if (catalog == 2) {
			catalogNode.setUsername(words[1]);
		} else if (catalog == 3) {
			catalogNode.setPassword(words[1]);
		} else if (catalog == 4) {
			numnodes = Integer.parseInt(words[1]);
		} else if (catalog == 5) {
			table = words[1];
		} else if (catalog == 6) {
			//partition = partition + line.charAt(x);
			temp = words[1];
			
			if(temp.equals("range")) {
				partition = 1;
			} else if (temp.equals("hash")) {
				partition = 2;
			} else {
				partition = 0;
			}
		} else if (catalog == 7) {
			partitionCol = words[1];
		} else if (catalog == 8) {
			partitionParam1 = words[1];
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
	
	/* Function readDDL
	 * Parameter: (String) ddl : filename of the ddlfile
	 * 
	 * Description: Takes a ddlfilename and gets the query information.
	 * 				the function will push the queries into sqlAL for
	 * 				future use. 
	 * 
	 * Returns: void
	*/
	// private static void readDDL(String ddl, ArrayList<String> arrayList) {
	private static void readSQL(String sql) {
		String line = "";
		int c;
		char ch;
		FileReader fr;
		BufferedReader br;

		try {
			fr = new FileReader(sql);
			br = new BufferedReader(fr);

			while ((c = br.read()) != -1) {
				ch = (char) c;

				if (ch == ';') {
					// System.out.println(line);
					sqlAL.add(line);
					line = "";
				} else if (ch != '\n') {
					line = line + ch;
				}

				// System.out.println(line);
			}
		} catch (IOException e) {
			System.out.println("File not found");
		}

		// return(sql);
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
	private static void readCatalog(String[] tables, DBNode catalogNode) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "";
		
		String nodeDriver = "";
		String nodeHostName = "";
		String nodeUserName = "";
		String nodePassword = "";
		String tname = "";
		int part= 0;
		
		if(numnodes == -1) {
			numnodes = 0;
		}
		
		try {
			// STEP 2: Register JDBC driver
			Class.forName(catalogNode.getDriver());
			
			// STEP 3: Open a connection
			if (catalogNode.getUsername().equals(" ") && catalogNode.getPassword().equals(" ")) {
				conn = DriverManager.getConnection(catalogNode.getHostname());
			} else {
				conn = DriverManager.getConnection(catalogNode.getHostname(), catalogNode.getUsername(), catalogNode.getPassword());
			}
			

			// STEP 4: Execute a query
			stmt = conn.createStatement();

			for(int i = 0;i < tables.length;i++) {
				if(tables[i] == null) {
					break;
				}
				
				sql = "SELECT * FROM dtables WHERE tname='" + tables[i]+ "' ORDER BY nodeid ";
				//"SELECT * FROM dtables WHERE tname=??? ORDER BY nodeid"
				//System.out.println(sql);
				
				// STEP 5: Extract data from result set
				rs = stmt.executeQuery(sql);
				
				System.out.println("Catalog read");
				
				numnodes = 0;
				// STEP 5: Extract data from result set
				while (rs.next()) {
					
					// Retrieve by column name
					nodeDriver = rs.getString("nodedriver");
					nodeHostName = rs.getString("nodeurl");
					nodeUserName = rs.getString("nodeuser");
					nodePassword = rs.getString("nodepasswd");
					tname = rs.getString("tname");
					part = rs.getInt("partmtd");
					
					nodeDriver = nodeDriver.trim().replaceAll(" +", "");
					nodeHostName = nodeHostName.trim().replaceAll(" +", "");
					nodeUserName = nodeUserName.trim().replaceAll(" +", "");
					nodePassword = nodePassword.trim().replaceAll(" +", "");
					tname = tname.trim().replaceAll(" +", "");
					
					if(nodeUserName.equals("")) {
						nodeUserName = " ";
					}
					if(nodePassword.equals("")) {
						nodePassword = " ";
					}	
					
					// Display values
					
					/*
					System.out.println("Driver: " + nodeDriver);
					System.out.println("URL: " + nodeHostName);
					System.out.println("User: " + nodeUserName);
					System.out.println("Pass: " + nodePassword);
					System.out.println("tname: " + tname);
					System.out.println("partmtd: " + part);
					*/
					
					//DBNode newNode = new DBNode(nodeDriver, nodeHostName, nodeUserName, nodePassword);
					//newNode.setTable(tname);
					//newNode.setPartitionMethod(part);
					//numnodes++;
					
					if (nodeAL1.size() <= numnodes) {
						
						DBNode newNode = new DBNode(nodeDriver, nodeHostName, nodeUserName, nodePassword);
						newNode.setTable(tname);
						newNode.setPartitionMethod(part);
						
						if(i == 0) {
							nodeAL1.add(newNode);
							System.out.println("added Node");
						} else if (i == 1) {
							nodeAL2.add(newNode);
							System.out.println("added Node");
						} else {
							System.out.println("ERROR! MORE THAN TWO TABLE!");
						}
					} else {
						nodeAL1.get(numnodes).setDriver(nodeDriver);
						nodeAL1.get(numnodes).setHostname(nodeHostName);
						nodeAL1.get(numnodes).setUsername(nodeUserName);
						nodeAL1.get(numnodes).setPassword(nodePassword);
					}
					
					numnodes++;
					
					
					
				} //end of while loop
			} // end of for loop
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
					"[" + catalogNode.getHostname().substring(0, catalogNode.getHostname().length()) + "]: catalog read failed");
		} catch (Exception e) {
			// Handle errors for Class.forName
			// e.printStackTrace();
			System.out.println(
					"[" + catalogNode.getHostname().substring(0, catalogNode.getHostname().length()) + "]: catalog read failed");
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
	
	/* Function cleanNodeAL
	 * Parameter: none
	 * 
	 * Description: Removes nodes in nodeAL that has no driver info. This is
	 * 				to weed out nodes that has incomplete information.
	 * 
	 * Returns: void
	*/
	private static void cleanNodeAL() {
		
		for(int x = 0; x < nodeAL1.size();x++) {
			if(nodeAL1.get(x).getDriver() == null) {
				nodeAL1.remove(x);
				x--;
			}
		}
		for(int x = 0; x < nodeAL2.size();x++) {
			if(nodeAL2.get(x).getDriver() == null) {
				nodeAL2.remove(x);
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
			
			for(int y = 0; y < nodeAL1.size();y++) {
				SqlFunc.runSQL(nodeAL1.get(y).getDriver(), nodeAL1.get(y).getHostname(), nodeAL1.get(y).getUsername(), nodeAL1.get(y).getPassword(), sql);
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
			
			
			for(int y = 0; y < nodeAL1.size();y++) {
				int colval = Integer.parseInt(csvData.get(x)[column]);
					
				if (nodeAL1.get(y).getPartitionParam1().equals("-inf")) {
						
					if (nodeAL1.get(y).getPartitionParam2().equals("+inf")) {
						SqlFunc.runSQL(nodeAL1.get(y).getDriver(), nodeAL1.get(y).getHostname(), nodeAL1.get(y).getUsername(), nodeAL1.get(y).getPassword(), sql);
					} else if(colval <= Integer.parseInt(nodeAL1.get(y).getPartitionParam2())) {
						SqlFunc.runSQL(nodeAL1.get(y).getDriver(), nodeAL1.get(y).getHostname(), nodeAL1.get(y).getUsername(), nodeAL1.get(y).getPassword(), sql);
					}
				} else if (nodeAL1.get(y).getPartitionParam2().equals("+inf")) {
					if (Integer.parseInt(nodeAL1.get(y).getPartitionParam1()) < colval) {
						SqlFunc.runSQL(nodeAL1.get(y).getDriver(), nodeAL1.get(y).getHostname(), nodeAL1.get(y).getUsername(), nodeAL1.get(y).getPassword(), sql);
					}
				} else {
					int p1 = Integer.parseInt(nodeAL1.get(y).getPartitionParam1());
					int p2 =  Integer.parseInt(nodeAL1.get(y).getPartitionParam2());
						
					if(p1 < colval && colval <= p2) {
						SqlFunc.runSQL(nodeAL1.get(y).getDriver(), nodeAL1.get(y).getHostname(), nodeAL1.get(y).getUsername(), nodeAL1.get(y).getPassword(), sql);
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
			i = ( p1 % nodeAL1.size());
				
			dr = nodeAL1.get(i).getDriver();
			hn = nodeAL1.get(i).getHostname();
			un = nodeAL1.get(i).getUsername();
			pw = nodeAL1.get(i).getPassword();
				
			SqlFunc.runSQL(dr, hn, un, pw, sql);
				
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
			
			if(data.equals("\"\"")) {
				data = null;
			} else if (data.charAt(0) == '"' && data.charAt(data.length() - 1) == '"') { //replaces double quote with single quote
				data = "'" + data.substring(1, data.length() -1)  + "'";
			}
			
			if(x > 0) {
				sql = sql + ",";
			}
			
			sql = sql + data;
		}
		sql = sql + ")";
		
		//System.out.println(sql);
		
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
		
		
		String JDBC_DRIVER = nodeAL1.get(0).getDriver();
		String DB_URL = nodeAL1.get(0).getHostname();
		String USER = nodeAL1.get(0).getUsername();
		String PASS = nodeAL1.get(0).getPassword();
		
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

	/* Function doThread
	 * Parameter: none
	 * 
	 * Description: runs multi threading.
	 * 
	 * Returns: void
	*/
	static void doThread() throws InterruptedException {
		threadMessage("Starting MessageLoop thread");
		long startTime = System.currentTimeMillis();
		// int nthreads=2;
		int nthreads = numnodes;
		Thread[] tList = new Thread[nodeAL1.size()];
		DBNode node1 = null;
		DBNode node2 = null;

		// for (int i = 0; i < nthreads; i++) {
		for (int i = 0; i < nodeAL1.size(); i++) {
			node1 = nodeAL1.get(i);
			
			if(nodeAL2.size() > 0) {
				for(int j = 0; j < nodeAL2.size(); j++) {
					node2 = nodeAL2.get(j);
					tList[i] = new Thread(new ThreadSQL(node1, node2));
					tList[i].start();
				}
			} else {
				tList[i] = new Thread(new ThreadSQL(node1, null));
				tList[i].start();
			}
			node1 = null;
			node2 = null;
		}

		threadMessage("Waiting for all MessageLoop threads to finish");

		// for (int i = 0; i < nthreads; i++) {
		for (int i = 0; (i < nthreads) && (i < nodeAL1.size()); i++) {
			tList[i].join();
		}

		threadMessage("Finally all done!");

	}

	// Display a message, preceded by the name of the current thread
	static void threadMessage(String message) {
		String threadName = Thread.currentThread().getName();
		System.out.format("%s: %s%n", threadName, message);
	}
	
	private static class ThreadSQL implements Runnable {

		private DBNode node1 = null;
		private DBNode node2 = null;

		ThreadSQL(DBNode n1, DBNode n2) {
			node1 = n1;
			node2 = n2;
		}

		public void run() {
			try {
				boolean tempBol = false;
				// int threadnum = (int) threadName;
				// System.out.println("Driver: " + JDBC_DRIVER);
				// System.out.println("hostname: " + DB_URL);
				// System.out.println("username: " + USER);
				// System.out.println("passord: " + PASS);
				
				
				for (int i = 0; i < sqlAL.size(); i++) {
					// Pause for 4 seconds
					Thread.sleep(5000);
					// Print a message
					threadMessage(sqlAL.get(i));
					
					
					if(ddl) {
						tempBol = SqlFunc.runDDL(node1.getDriver(), node1.getHostname(), node1.getUsername(), node1.getPassword(), sqlAL.get(i));
						
						if(successful == false && tempBol == true) {
							successful = true;
						}
					} else if(node2 == null) {
						SqlFunc.runSQL(node1.getDriver(), node1.getHostname(), node1.getUsername(), node1.getPassword(), sqlAL.get(i));
					} else {
						SqlFunc.runJoin(node1, node2, sqlAL.get(i));
					}
				}
			} catch (InterruptedException e) {
				threadMessage("I wasn't done!");
			}
		}
	}

}// end of class
