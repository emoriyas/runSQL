//package runDDL;

public class DBNode {

	
	/*
	 * node1.driver=com.ibm.db2.jcc.DB2Driver
node1.hostname=jdbc:db2://10.0.0.3:50001/mydb1
node1.username=db2inst1
node1.passwd=mypasswd
	 */
	
	private String driver;
	private String hostname;
	private String username;
	private String password;
	
	public DBNode(String dr, String hn, String un, String pw) {
		
		driver = dr;
		hostname = hn;
		username = un;
		password = pw;
		
	}
	
	
	public String getDriver(){
		return(driver);
	}
	
	public String getHostName(){
		return(hostname);
	}
	
	public String getUserName(){
		return(username);
	}
	
	public String getPassword(){
		return(password);
	}
	
}
