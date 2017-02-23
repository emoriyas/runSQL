//package runDDL;

//import java.io.IOException;
//import java.io.FileReader;
//import java.io.BufferedReader;
import java.io.*;
import java.util.*;
import java.lang.String;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//import java.sql.ResultSetMetaData;

/**
 * Author: Eric Moriyasu ICS 421 Assignment 2
 * 
 * reads from clustercfg and sqlfile to run a multi threaded sql queries
 * 
 * Jan. 23 2017
 */


public class Main {

	// varaibles to hold clustercfg data
	static String catalogDriver = "";
	static String catalogHostName = "";
	static String catalogUserName = "";
	static String catalogPassword = "";
	static ArrayList<String> sqlAL = new ArrayList<String>();
	static ArrayList<DBNode> nodeAL = new ArrayList<DBNode>();
	static boolean successful = false;
	static String command = "";

	static int numnodes = -1;

	
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

		FileReader fr;
		BufferedReader br;

		// int numnodes = 0;

		if (args.length > 1) {
			clustercfg = args[0];
			sqlfile = args[1];

			try {
				fr = new FileReader(clustercfg);
				br = new BufferedReader(fr);

				while ((line = br.readLine()) != null) {
					System.out.println(line);

					processCatalogLine(line);
				} // end of while
				
				// String tname = "";
				
				readSQL(sqlfile);
				readCatalog(parseTname(sqlAL.get(0)));
				doThread();

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
				// numnodes = (int) line.substring(x);

				numnodes = Integer.parseInt(line.substring(x));

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
			successful = true;

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
		
		if(numnodes == -1) {
			numnodes = 0;
		}
		

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
			System.out.println(sql);
			
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
				
				DBNode newNode = new DBNode(nodeDriver, nodeHostName, nodeUserName, nodePassword);
				numnodes++;

				nodeAL.add(newNode);
				
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
	
	/* Function parseTname
	 * Parameter: (String) sql : sql query to extract information from
	 * 
	 * Description: gets the table name in query
	 * 
	 * Returns: (String) : The table name
	*/
	private static String parseTname(String sql) {
		String ret = "";
		// String[] ret = str.split("name |field |grade ");

		char ch;
		String word = "";
		int control = 0;

		for (int x = 0; x < sql.length(); x++) {
			ch = sql.charAt(x);
			// word = word + ch;

			if (control == 1) {
				if (ch == '(' || ch == ' ') {
					control++;
				} else {
					ret = ret + ch;
				}
			} else if (control == 0 && word.equalsIgnoreCase("TABLE")) {
				control++;
			} else if (control == 0 && word.equalsIgnoreCase("FROM")) {
				control++;
			} else if (control == 0 && word.equalsIgnoreCase("INTO")) {
				control++;	
			} else if (control == 0 && word.equalsIgnoreCase("UPDATE")) {
				control++;
			} else if (ch == ' ') {
				word = "";
			} else {
				word = word + ch;
			}
		}
		System.out.println(ret);
		return (ret);
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
		Thread[] tList;

		tList = new Thread[nthreads];
		String driver = null;
		String host = null;
		String user = null;
		String pass = null;

		// for (int i = 0; i < nthreads; i++) {
		//System.out.println(nodeAL.size());
		for (int i = 0; (i < nthreads) && (i < nodeAL.size()); i++) {
			driver = (nodeAL.get(i)).getDriver();
			host = (nodeAL.get(i)).getHostName();
			user = (nodeAL.get(i)).getUserName();
			pass = (nodeAL.get(i)).getPassword();
			// tList[i]= new Thread( new MessageLoop() );
			// System.out.println("HN is: " + host + i);
			tList[i] = new Thread(new MessageLoop(driver, host, user, pass));
			tList[i].start();
		}

		threadMessage("Waiting for all MessageLoop threads to finish");

		// for (int i = 0; i < nthreads; i++) {
		for (int i = 0; (i < nthreads) && (i < nodeAL.size()); i++) {
			tList[i].join();
		}

		threadMessage("Finally all done!");

	}

	// Display a message, preceded by the name of the current thread
	static void threadMessage(String message) {
		String threadName = Thread.currentThread().getName();
		System.out.format("%s: %s%n", threadName, message);
	}
	
	private static class MessageLoop implements Runnable {

		private String JDBC_DRIVER;
		private String DB_URL;
		private String USER;
		private String PASS;

		MessageLoop(String driver, String url, String user, String pass) {
			JDBC_DRIVER = driver;
			DB_URL = url;
			USER = user;
			PASS = pass;

			// System.out.println("in constructor, param is" + url);
			// System.out.println("in constructor, hn is" + DB_URL);
		}

		public void run() {
			try {
				// int threadnum = (int) threadName;
				// System.out.println("Driver: " + JDBC_DRIVER);
				// System.out.println("hostname: " + DB_URL);
				// System.out.println("username: " + USER);
				// System.out.println("passord: " + PASS);

				for (int i = 0; i < sqlAL.size(); i++) {
					// Pause for 4 seconds
					Thread.sleep(4000);
					// Print a message
					threadMessage(sqlAL.get(i));
					runSQL(JDBC_DRIVER, DB_URL, USER, PASS, sqlAL.get(i));

				}
			} catch (InterruptedException e) {
				threadMessage("I wasn't done!");
			}
		}
	}

}// end of class
