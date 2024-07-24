package app.trigyn.utility.dbUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

public class SchemaCompare {
	
	
	private static boolean isSkipRecordCount = true;
	
	private static final Properties loadProperties()  throws Throwable {
		String configFileNameName = "config.properties"; // could also be a constant
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		
		Properties props = new Properties();
		 // Try to load from resources folder first
        try (InputStream input = SchemaCompare.class.getClassLoader().getResourceAsStream(configFileNameName)) {
            if (input != null) {
            	props.load(input);
                System.out.println("Loaded properties from resources folder.");
            } else {
                System.out.println(configFileNameName + " not found in resources folder. Trying to load from same path as JAR.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
     // If not found in resources, try to load from the same path as the JAR
        if (props.isEmpty()) {
            try {
                String jarDir = new File(SchemaCompare.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent();
                File propertiesFile = new File(jarDir, configFileNameName);

                if (propertiesFile.exists() && propertiesFile.canRead()) {
                    try (FileInputStream input = new FileInputStream(propertiesFile)) {
                    	props.load(input);
                        System.out.println("Loaded properties from the same path as the JAR.");
                    }
                } else {
                    System.out.println(configFileNameName + " not found in the same path as the JAR.");
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        
        String[] propertyKeys = {"loadRecordCount", "sourceURL", "targetURL", "sourceUserName", "targetUserName", "sourcePassword", "targetPassword", "sourceSchema", "targetSchema", "reportPath", "addTimeStamp"};

        if(props.isEmpty()) {
			System.err.println("Could not load required properties. Exiting system");
			System.exit(-1);
		}
        
        for(String pKey : propertyKeys) {
        	if(props.containsKey(pKey) == false) {
        		System.err.println(pKey + " is not provided. Exiting system. All required properties are mentioned below.");
        		System.out.println(Arrays.toString(propertyKeys));
        		System.exit(-1);
        	}
        }
        
        String reportPath = props.getProperty("reportPath");
        File reportFolder = new File(reportPath);
        if(reportFolder == null || reportFolder.isDirectory() == false || 
        		reportFolder.exists() == false || reportFolder.canWrite() == false) {
        	System.err.println("Resport output folder is not proper : " + reportPath);
        	System.exit(-1);
        }
                
		return props;
	}
	
	public static void main(String[] args) throws Throwable {
		System.out.println("SchemaCompare.main()");

		Properties props = loadProperties();
		System.out.println(props);
		
		
		isSkipRecordCount = "true".equalsIgnoreCase(props.getProperty("loadRecordCount")) == false;
		
		ConnectionDetails source = new ConnectionDetails(props.getProperty("sourceURL"), props.getProperty("sourceUserName"), props.getProperty("sourcePassword"), props.getProperty("sourceSchema"));
		ConnectionDetails target = new ConnectionDetails(props.getProperty("targetURL"), props.getProperty("targetUserName"), props.getProperty("targetPassword"), props.getProperty("targetSchema"));

		Connection sourceCon = getConnection(source) ;
		DataBaseType sourceType = getDataBaseType(sourceCon);
		
		Connection targetCon = getConnection(target) ;
		DataBaseType targetType = getDataBaseType(targetCon);
		
		if(sourceType != targetType) {
			System.err.println("Source and target are not same Database type " + sourceType + " <-> " + targetType);
			return;
		}
		
		DBCompareStructure	dbCompare = getDBCompareStructure(sourceCon, targetCon, source.getSchemaName(), target.getSchemaName());
		dbCompare.sourceConnection = source;
		dbCompare.targetConnection = target;
		
		dbCompare.sourceDBType = sourceType;
		dbCompare.targetDBType = targetType;
		
		compareDBStructure(dbCompare);
		listDatabaseObjects(dbCompare);
		publishComparison(dbCompare, props);
	}
	
	private static void publishComparison(DBCompareStructure a_dbCompareStructure, Properties a_applicationProperties) throws Throwable {
		new Publisher(a_dbCompareStructure, a_applicationProperties).publish();
	}
	
	private static final void listDatabaseObjects(DBCompareStructure a_dbCompareStructure) throws Throwable {
		ResultSet	rs	= a_dbCompareStructure.sourceDBMetaData.getTables(a_dbCompareStructure.sourceConnection.getSchemaName(),
				a_dbCompareStructure.sourceConnection.getSchemaName(), "%", null);
//		System.out.println("Source=================");
		while (rs.next()) {
			if("TABLE".equalsIgnoreCase(rs.getString("TABLE_TYPE"))) {
				continue;
			}
//			System.out.println(rs.getString(3) + " is " + rs.getString("TABLE_TYPE"));
			a_dbCompareStructure.sourceObjects.put(rs.getString(3), rs.getString("TABLE_TYPE"));
		}
//		System.out.println("Target=================");
		rs	= a_dbCompareStructure.targetDBMetaData.getTables(a_dbCompareStructure.targetConnection.getSchemaName(),
				a_dbCompareStructure.targetConnection.getSchemaName(), "%", null);
		while (rs.next()) {
			if("TABLE".equalsIgnoreCase(rs.getString("TABLE_TYPE"))) {
				continue;
			}
//			System.out.println(rs.getString(3) + " is " + rs.getString("TABLE_TYPE"));
			a_dbCompareStructure.targetObjects.put(rs.getString(3), rs.getString("TABLE_TYPE"));
		}
	}
	
	private static void compareDBStructure(DBCompareStructure a_dbCompareStructure) {
		System.out.println("SchemaCompare.compareDBStructure()");
		
		if(a_dbCompareStructure.sourceTables.keySet().containsAll(a_dbCompareStructure.targetTables.keySet()) &&
				a_dbCompareStructure.targetTables.keySet().containsAll(a_dbCompareStructure.sourceTables.keySet())) {
			System.out.println("Both database have equal tables");
		}else {
			System.out.println("Both database doesn't have equal tables");
			for(String tableName : a_dbCompareStructure.sourceTables.keySet()) {
				if(a_dbCompareStructure.targetTables.keySet().contains(tableName) == false) {
					//System.err.println(tableName + " is present in source but not present in target");
					a_dbCompareStructure.onlySourceTables.add(tableName);
				}
			}
			
			for(String tableName : a_dbCompareStructure.targetTables.keySet()) {
				if(a_dbCompareStructure.sourceTables.keySet().contains(tableName) == false) {
					//System.err.println(tableName + " is present in target but not present in source");
					a_dbCompareStructure.onlyTargetTables.add(tableName);
				}
			}
		}
	}
	
	private static DBCompareStructure getDBCompareStructure(Connection sourceCon, Connection targetCon, String sourceSchemaName, String targetSchemaName) throws Throwable  {
		System.out.println("SchemaCompare.getDBCompareStructure()");
		
		DBCompareStructure	dbCompare	= new DBCompareStructure();
		
		DatabaseMetaData	dBMetaData	= sourceCon.getMetaData();
		dbCompare.sourceDBMetaData = dBMetaData;
		ResultSet			rs			= dBMetaData.getTables(sourceSchemaName, sourceSchemaName, "%", new String[]{"TABLE"});
		ResultSet rsTable = null;
		String tableName = null;
		System.out.println();
		while (rs.next()) {
			
			if(rs.getString("TABLE_TYPE").equalsIgnoreCase("TABLE") == false) {
				System.out.println("Ignoring " + rs.getString(3));
				continue;
			}
			
			tableName = rs.getString(3);
			System.out.print("Analyzing : " + tableName);
			try {
				if(isSkipRecordCount) {
					dbCompare.sourceTablesRow.put(tableName, 0);	
				}else {
					if(getDataBaseType(sourceCon) == DataBaseType.PostgreSQL) {
						rsTable = sourceCon.createStatement().executeQuery("select count(*) from " + sourceSchemaName + ".\"" + tableName + "\"");	
					}else {
						rsTable = sourceCon.createStatement().executeQuery("select count(*) from " + sourceSchemaName + "." + tableName);
					}
					
					rsTable.next();
					dbCompare.sourceTablesRow.put(tableName, rsTable.getInt(1));
				}
				
				if(getDataBaseType(sourceCon) == DataBaseType.PostgreSQL) {
					dbCompare.sourceTables.put(tableName, sourceCon.prepareStatement("select * from " + sourceSchemaName + ".\"" + tableName + "\"").getMetaData());	
				}else {
					dbCompare.sourceTables.put(tableName, sourceCon.prepareStatement("select * from " + sourceSchemaName + "." + tableName).getMetaData());
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(e.getMessage());
			}
			System.out.print(" : Done\n");
		}

		System.out.println("SchemaCompare.getDBCompareStructure(1)");
		
		dBMetaData	= targetCon.getMetaData();
		dbCompare.targetDBMetaData = dBMetaData;
		rs			= dBMetaData.getTables(targetSchemaName, targetSchemaName, "%",  new String[]{"TABLE"});
		while (rs.next()) {
			if(rs.getString("TABLE_TYPE").equalsIgnoreCase("TABLE") == false) {
				System.out.println("Ignoring " + rs.getString(3));
				continue;
			}
			
			tableName = rs.getString(3);
			System.out.print("Analyzing : " + tableName);
			//System.out.println(rs.getString(3));
			try {
				if(isSkipRecordCount) {
					dbCompare.targetTablesRow.put(tableName, 0);
				}else {
					if(getDataBaseType(sourceCon) == DataBaseType.PostgreSQL) {
						rsTable = targetCon.createStatement().executeQuery("select count(*) from " + targetSchemaName + ".\"" + tableName + "\"");	
					}else {
						rsTable = targetCon.createStatement().executeQuery("select count(*) from " + targetSchemaName + "." + tableName);
					}
					
					rsTable.next();
					dbCompare.targetTablesRow.put(tableName, rsTable.getInt(1));
				}
				
				if(getDataBaseType(sourceCon) == DataBaseType.PostgreSQL) {
					dbCompare.targetTables.put(rs.getString(3), targetCon.prepareStatement("select * from " + targetSchemaName + ".\"" + tableName + "\"").getMetaData());	
				}else {
					dbCompare.targetTables.put(rs.getString(3), targetCon.prepareStatement("select * from " + targetSchemaName + "." + tableName).getMetaData());
				}
				
			} catch (Exception e) {
				//e.printStackTrace();
				System.err.println(e.getMessage());
			}
			System.out.print(" : Done\n");
		}
		return dbCompare;
	}
	
	private static Connection getConnection(ConnectionDetails a_connectionDetails) throws Throwable {
		System.out.println("SchemaCompare.getTargetConnection()");
		if(a_connectionDetails.getConnectionString().contains("postgresql")) {
			Class.forName("org.postgresql.Driver");
		}
		Connection con = DriverManager.getConnection(a_connectionDetails.getConnectionString(),
				a_connectionDetails.getUserName(), a_connectionDetails.getPassword());
		return con;
	}
	
	private static DataBaseType getDataBaseType(Connection a_con) throws Throwable {
		DatabaseMetaData dBMetaData = a_con.getMetaData();
		String dbName = dBMetaData.getDatabaseProductName();
		System.out.println(dbName);
		if(dbName == null || dbName.trim().length() < 1) {
			return DataBaseType.Undefined;
		}
		
		if(dbName.toLowerCase().contains("mysql")) {
			return DataBaseType.MySQL;
		}else if("PostgreSQL".equalsIgnoreCase(dbName)) {
			return DataBaseType.PostgreSQL;
		}
		
		return DataBaseType.Undefined;
	}
}
