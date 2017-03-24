
public class SqlParse {

	/* Function parseTname
	 * Parameter: (String) sql : sql query to extract information from
	 * 
	 * Description: gets the table name in query
	 * 
	 * Returns: (String[]) : The table name
	*/
	public static String[] getTable(String sql) {
		String[] ret = new String[2];
		// String[] ret = str.split("name |field |grade ");

		String[] strArr = sql.split("\\s+");
		
		char ch;
		String word = "";
		int control = 0;

		for(int x = 0; x < strArr.length; x++) {
			
			//System.out.println(strArr[x]);
			
			
			if(control == 1) {
				
				//System.out.println(strArr[x]);
				
				if(strArr[x].charAt(strArr[x].length() - 1) == ',' || strArr[x].equalsIgnoreCase("JOIN")) {
					ret[1] = strArr[x + 1];
					x = strArr.length + 1;
				} else if (strArr[x].equalsIgnoreCase("WHERE")) {
					x = strArr.length + 1; 
				}
			} else if(strArr[x].equalsIgnoreCase("TABLE") || strArr[x].equalsIgnoreCase("FROM")) {
				if(strArr[x + 1].charAt(strArr[x + 1].length() - 1) == ',') {
					ret[0] = strArr[x + 1].substring(0, strArr[x + 1].length() - 1);
					ret[1] = strArr[x + 2];
					x = strArr.length + 1;
				} else {
					ret[0] = strArr[x + 1];
					control++;
				}
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

		String[] strArr = sql.split("\\s+");
		ret = strArr[0];

		return (ret);
	}
	
	public static boolean isDDL(String sql) {
		
		boolean ret = false;
		String cmd = getCmd(sql);
		
		if(cmd.equalsIgnoreCase("CREATE")) {
			ret = true;
		} else if(cmd.equalsIgnoreCase("ALTER")) {
			ret = true;
		} else if(cmd.equalsIgnoreCase("DROP")) {
			ret = true;
		} else if(cmd.equalsIgnoreCase("TRUNCATE")) {
			ret = true;
		} else if(cmd.equalsIgnoreCase("COMMENT")) {
			ret = true;
		} else if(cmd.equalsIgnoreCase("RENAME")) {
			ret = true;
		}
		
		return(ret);
	}

	
}
