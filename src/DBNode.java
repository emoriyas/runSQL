//package runDDL;

public class DBNode {
	
	private String driver = null;
	private String hostname = null;
	private String username = null;
	private String password = null;
	//private String partitionMethod;
	//private String partitionColumn;
	private int partitionMethod = 0;
	private String partitionParam1 = null;
	private String partitionParam2 = null;
	private String table = null;
	
	
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
	
	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public int getPartitionMethod() {
		return partitionMethod;
	}

	public void setPartitionMethod(int partitionMethod) {
		this.partitionMethod = partitionMethod;
	}
	
}
