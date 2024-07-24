package app.trigyn.utility.dbUtil;

public class ConnectionDetails {
	private String	connectionString	= null;
	private String	userName			= null;
	private String	password			= null;
	private String	schemaName			= null;

	public ConnectionDetails(String a_connectionString, String a_userName, String a_password, String a_schemaName) {
		connectionString	= a_connectionString;
		userName			= a_userName;
		password			= a_password;
		schemaName			= a_schemaName;
	}
	
	public String getConnectionString() {
		return connectionString;
	}

	public void setConnectionString(String a_connectionString) {
		connectionString = a_connectionString;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String a_userName) {
		userName = a_userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String a_password) {
		password = a_password;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String a_schemaName) {
		schemaName = a_schemaName;
	}
}
