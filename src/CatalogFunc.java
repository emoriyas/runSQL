
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CatalogFunc {

	/* Function runCatalog
	 * Parameter: none
	 * 
	 * Description: attempts to establish a database connection to the catalog DB
	 * 				and run update to metadata table. Will close connection after 
	 * 				update is completed
	 * 
	 * Returns: void
	*/
	public static void updateCatalogDDL(DBNode catalogNode, ArrayList<DBNode> nodeAL, ArrayList<String> sqlAL) {

		Connection conn = null;
		Statement stmt = null;
		String sql = "";
		boolean tableExist = true;
		String[] tname = null;
		String cmd = "";

		String catalogDriver = catalogNode.getDriver();
		String catalogHostName = catalogNode.getHostname();
		String catalogUserName = catalogNode.getUsername();
		String catalogPassword = catalogNode.getPassword();
		
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

			try {

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

			tname = SqlParse.getTable(sqlAL.get(0));
			cmd = SqlParse.getCmd(sqlAL.get(0));
			
			for (int x = 0; x < nodeAL.size(); x++) {

				System.out.println("Updating catalog for node" + (x + 1));

				// System.out.println(cmd);

				if (cmd.equals("CREATE")) {

					sql = "INSERT INTO dtables VALUES " + "('" + tname[0] + "', " + "'" + (nodeAL.get(x)).getDriver()
							+ "', " + "'" + (nodeAL.get(x)).getHostname() + "', " + "'" + (nodeAL.get(x)).getUsername()
							+ "', " + "'" + (nodeAL.get(x)).getPassword() + "', " + -1 + ", " + "" + x + ", " + null
							+ ", " + null + ", " + null + ")";

					
					stmt.executeUpdate(sql);
				} else if (cmd.equals("DROP")) {
					sql = "DELETE FROM dtables WHERE " + "tname = '" + tname[0] + "'" + "AND nodedriver = '"
							+ (nodeAL.get(x)).getDriver() + "' " + "AND nodeurl = '" + (nodeAL.get(x)).getHostname()
							+ "' " + "AND nodeuser = '" + (nodeAL.get(x)).getUsername() + "' " + "AND nodepasswd = '"
							+ (nodeAL.get(x)).getPassword() + "' "
							// + "AND partmtd = " + -1 + "'"
							+ "AND nodeid = " + x + ""
							// + "AND partcol = '" + null + "'"
							// + "AND partparam1 = '" + null + "'"
							// + "AND partparam2 = '" + null
							+ "";

					stmt.executeUpdate(sql);
					System.out.println(sql);
				}

			}

			// rs.close();
			stmt.close();
			conn.close();
			System.out
					.println("[" + catalogHostName + "]: Catalog Updated");
		} catch (SQLException se) {
			// Handle errors for JDBC
			// se.printStackTrace();
			System.out.println(
					"[" + catalogHostName + "]: catalog update failed");
		} catch (Exception e) {
			// Handle errors for Class.forName
			// e.printStackTrace();
			System.out.println(
					"[" + catalogHostName + "]: catalog update failed");
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
	
}
