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
import java.sql.SQLException;
import java.sql.Statement;
//import java.sql.ResultSetMetaData;

/**
 * Author: Eric Moriyasu ICS 421 Assignment 1 
 * 
 * reads from clustercfg and ddlfile to run a multi threaded ddl queries
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
		String ddlfile = "";
		String line = null;
		String wordLeft = ""; // left side of catalog eqs
		String wordRight = ""; // right side of catalog eqs

		// ArrayList to store the node information
		// ArrayList<DBNode> nodeAL = new ArrayList<DBNode>();
		int nodeCount = 1; // for use in getting values for nodes
		int temp = 0;

		String nodeDriver = "";
		String nodeHostName = "";
		String nodeUserName = "";
		String nodePassword = "";

		/*
		 * switch for scanning catalogs. -1 = nothing 0 = driver, 1=hostname,
		 * 2=username, 3=passwd
		 *
		 */

		FileReader fr;
		BufferedReader br;

		// int numnodes = 0;

		if (args.length > 1) {
			clustercfg = args[0];
			ddlfile = args[1];

			try {
				fr = new FileReader(clustercfg);
				br = new BufferedReader(fr);

				while ((line = br.readLine()) != null) {
					// System.out.println(line);

					if (numnodes == -1) {
						processCatalogLine(line);
					} else {

						// System.out.println(line);

						for (int x = 0; x < line.length(); x++) {
							// System.out.println(wordLeft);
							// System.out.println(temp);
							// System.out.println("node" + nodeCount +
							// "driver=");

							if (wordLeft.equals("node" + nodeCount + ".driver=")) {
								nodeDriver = nodeDriver + line.charAt(x);

								if (x == (line.length() - 1)) {
									temp++;
								}

							} else if (wordLeft.equals("node" + nodeCount + ".hostname=")) {
								nodeHostName = nodeHostName + line.charAt(x);

								if (x == (line.length() - 1)) {
									temp++;
								}

							} else if (wordLeft.equals("node" + nodeCount + ".username=")) {
								nodeUserName = nodeUserName + line.charAt(x);

								if (x == (line.length() - 1)) {
									temp++;
								}

							} else if (wordLeft.equals("node" + nodeCount + ".passwd=")) {
								nodePassword = nodePassword + line.charAt(x);

								if (x == (line.length() - 1)) {
									temp++;
								}
							} else {
								wordLeft = wordLeft + line.charAt(x);
							}

						}

					}

					if (temp == 4) {

						DBNode newNode = new DBNode(nodeDriver, nodeHostName, nodeUserName, nodePassword);

						nodeAL.add(newNode);

						nodeDriver = "";
						nodeHostName = "";
						nodeUserName = "";
						nodePassword = "";
						nodeCount++;
						temp = 0;
					}

					// System.out.println(wordLeft);
					wordLeft = "";
					// catalog = -1;
				} // end of while

				// String tname = "";

				readDDL(ddlfile);

				/*
				 * String derp =
				 * "CREATE TABLE BOOKS(isbn char(14), title char(80), price decimal)"
				 * ;
				 * 
				 * System.out.println(derp); System.out.print("herr drr: ");
				 * System.out.println(parseTname(derp));
				 */

				doThread();

				if (successful) {
					runCatalog();
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

	
	/*
	 This function is currently not in use	
	*/
	private static void processNodeLine(String line, int node) {

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
	private static void readDDL(String ddl) {
		String line = "";
		int c;
		char ch;
		FileReader fr;
		BufferedReader br;

		try {
			fr = new FileReader(ddl);
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
	private static void runDDL(String JDBC_DRIVER, String DB_URL, String USER, String PASS, String sql) {

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

			// conn = DriverManager.getConnection(DB_URL, USER, PASS);
			// conn = DriverManager.getConnection(DB_URL);

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

			System.out.println("[" + DB_URL.substring(0, (DB_URL.length() - 1)) + "]: sql success");
			successful = true;

			// rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			// se.printStackTrace();
			System.out.println("[" + DB_URL.substring(0, (DB_URL.length() - 1)) + "]: sql failed");
		} catch (Exception e) {
			// Handle errors for Class.forName
			// e.printStackTrace();
			System.out.println("[" + DB_URL.substring(0, (DB_URL.length() - 1)) + "]: sql failed");
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

	
	/* Function runCatalog
	 * Parameter: none
	 * 
	 * Description: attempts to establish a database connection to the catalog DB
	 * 				and run update to metadata table. Will close connection after 
	 * 				update is completed
	 * 
	 * Returns: void
	*/
	private static void runCatalog() {
		// runDDL(String JDBC_DRIVER, String DB_URL, String USER, String PASS,
		// String sql)

		System.out.println(sqlAL.get(0));

		/*
		 * static String catalogDriver = ""; static String catalogHostName = "";
		 * static String catalogUserName = ""; static String catalogPassword =
		 * "";
		 */
		Connection conn = null;
		Statement stmt = null;
		String sql = "";
		boolean tableExist = true;
		String tname = "";
		String cmd = "";

		try {
			// STEP 2: Register JDBC driver
			Class.forName(catalogDriver);

			// STEP 3: Open a connection
			conn = DriverManager.getConnection(catalogHostName);

			// STEP 4: Execute a query
			stmt = conn.createStatement();

			try {
				// String tname, String JDBC_DRIVER, String DB_URL, String USER,
				// String PASS, int nodeid

				sql = "SELECT * FROM dtables";
				stmt.executeQuery(sql);

				System.out.println("Catalog table already exists");

			} catch (SQLException e) {

				tableExist = false;

				System.out.println("Catalog table does not exist");
			}

			if (tableExist == false) {
				sql = "CREATE TABLE dtables(tname char(32)," + "nodedriver char(64)," + "nodeurl char(128),"
						+ "nodeuser char(16)," + "nodepasswd char(16)," + "partmtd int," + "nodeid int,"
						+ "partcol char(32)," + "partparam1 char(32)," + "partparam2 char(32))";

				stmt.executeUpdate(sql);

				System.out.println("SQL table created");
			}

			/*
			 * INSERT INTO SECONDTABLE VALUES (100,'ONE HUNDRED'),(200,'TWO
			 * HUNDRED'),(300,'THREE HUNDRED');
			 * 
			 * (String JDBC_DRIVER, String DB_URL, String USER, String PASS,
			 * String sql)
			 */

			for (int x = 0; x < nodeAL.size() - 1; x++) {

				tname = parseTname(sqlAL.get(x));

				cmd = getCmd(sqlAL.get(x));
				// System.out.println(cmd);

				if (cmd.equals("CREATE")) {

					sql = "INSERT INTO dtables VALUES " + "('" + tname + "', " + "'" + (nodeAL.get(x)).getDriver()
							+ "', " + "'" + (nodeAL.get(x)).getHostName() + "', " + "'" + (nodeAL.get(x)).getUserName()
							+ "', " + "'" + (nodeAL.get(x)).getPassword() + "', " + -1 + ", " + "" + x + ", " + null
							+ ", " + null + ", " + null + ")";

					stmt.executeUpdate(sql);
				} else if (cmd.equals("DROP")) {
					sql = "DELETE FROM dtables WHERE " + "tname = '" + tname + "'" + "AND nodedriver = '"
							+ (nodeAL.get(x)).getDriver() + "' " + "AND nodeurl = '" + (nodeAL.get(x)).getHostName()
							+ "' " + "AND nodeuser = '" + (nodeAL.get(x)).getUserName() + "' " + "AND nodepasswd = '"
							+ (nodeAL.get(x)).getPassword() + "' "
							// + "AND partmtd = " + -1 + "'"
							+ "AND nodeid = " + x + ""
							// + "AND partcol = '" + null + "'"
							// + "AND partparam1 = '" + null + "'"
							// + "AND partparam2 = '" + null
							+ "";

					stmt.executeUpdate(sql);
				}

				// stmt.executeQuery(sql);
				// stmt.executeUpdate(sql);

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
			} else if (control == 0 && word.equals("TABLE")) {
				control++;
			} else if (ch == ' ') {
				word = "";
			} else {
				word = word + ch;
			}
		}

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
					runDDL(JDBC_DRIVER, DB_URL, USER, PASS, sqlAL.get(i));

				}
			} catch (InterruptedException e) {
				threadMessage("I wasn't done!");
			}
		}
	}

}// end of class
