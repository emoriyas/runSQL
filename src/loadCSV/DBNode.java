package loadCSV;
//package runDDL;

public class DBNode {
	
	private String driver;
	private String hostname;
	private String username;
	private String password;
	//private String partitionMethod;
	//private String partitionColumn;
	private String partitionParam1;
	private String partitionParam2;
	
	public DBNode() {
	}

		
	public DBNode(String dr, String hn, String un, String pw) {
		
		setDriver(dr);
		setHostname(hn);
		setUsername(un);
		setPassword(pw);
		
	}
	
	public DBNode(String dr, String hn, String un, String pw, String pp1, String pp2) {
		
		setDriver(dr);
		setHostname(hn);
		setUsername(un);
		setPassword(pw);
		//setPartitionMethod(pm);
		//setPartitionColumn(pc);
		setPartitionParam1(pp1);
		setPartitionParam2(pp2);
		
	}
	
	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	/*
	public String getPartitionMethod() {
		return partitionMethod;
	}

	public void setPartitionMethod(String partitionMethod) {
		this.partitionMethod = partitionMethod;
	}

	public String getPartitionColumn() {
		return partitionColumn;
	}

	public void setPartitionColumn(String partitionColumn) {
		this.partitionColumn = partitionColumn;
	}
	*/
	public String getPartitionParam1() {
		return partitionParam1;
	}

	public void setPartitionParam1(String partitionParam1) {
		this.partitionParam1 = partitionParam1;
	}

	public String getPartitionParam2() {
		return partitionParam2;
	}

	public void setPartitionParam2(String partitionParam2) {
		this.partitionParam2 = partitionParam2;
	}
	
}
