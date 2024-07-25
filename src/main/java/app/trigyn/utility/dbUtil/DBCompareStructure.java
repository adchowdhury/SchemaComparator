package app.trigyn.utility.dbUtil;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class DBCompareStructure {
	
	public ConnectionDetails sourceConnection = null;
	public ConnectionDetails targetConnection = null;
	
	public DataBaseType sourceDBType = DataBaseType.Undefined;
	public DataBaseType targetDBType = DataBaseType.Undefined;
	
	public SortedMap<String, ResultSetMetaData> sourceTables = new TreeMap<String, ResultSetMetaData>(); 
	public SortedMap<String, ResultSetMetaData> targetTables = new TreeMap<String, ResultSetMetaData>(); 
	
	public SortedMap<String, Integer> sourceTablesRow = new TreeMap<String, Integer>(); 
	public SortedMap<String, Integer> targetTablesRow = new TreeMap<String, Integer>(); 
	

	public SortedMap<String, SortedMap<String, DBColumn>> sourceTableColumns = new TreeMap<String, SortedMap<String, DBColumn>>(); 
	public SortedMap<String, SortedMap<String, DBColumn>> targetTableColumns = new TreeMap<String, SortedMap<String, DBColumn>>(); 
	
	public SortedMap<String, SortedMap<String, DBColumnIndex>> sourceTableColumnIndexes = new TreeMap<String, SortedMap<String, DBColumnIndex>>(); 
	public SortedMap<String, SortedMap<String, DBColumnIndex>> targetTableColumnIndexes = new TreeMap<String, SortedMap<String, DBColumnIndex>>(); 
	
	public Set<String> onlySourceTables = new HashSet<String>();
	public Set<String> onlyTargetTables = new HashSet<String>();
	
	public DatabaseMetaData sourceDBMetaData = null;
	public DatabaseMetaData targetDBMetaData = null;
	
	public SortedMap<String, String> sourceObjects = new TreeMap<String, String>(); 
	public SortedMap<String, String> targetObjects = new TreeMap<String, String>(); 
	
	public final static boolean compareIndexes(DBCompareStructure a_dbCompareStructure, 
			String currentTableName) throws Throwable {
		boolean isEqual = true, nonUnique;
		ResultSet rs = null;
		String tempSchema = a_dbCompareStructure.sourceConnection.getSchemaName();
		if(tempSchema != null && tempSchema.trim().length() < 1) {
			tempSchema = null;
		}
		rs = a_dbCompareStructure.sourceDBMetaData.getIndexInfo(tempSchema, tempSchema,  currentTableName, false, true);
		
		while(rs.next()) {
			
			if(a_dbCompareStructure.sourceTableColumnIndexes.containsKey(currentTableName) == false) {
				a_dbCompareStructure.sourceTableColumnIndexes.put(currentTableName, new TreeMap<String, DBColumnIndex>());
			}
			
			DBColumnIndex tempCol = new DBColumnIndex().
					setColumnName(rs.getString("COLUMN_NAME")).
					setNonUnique(rs.getBoolean("NON_UNIQUE")).
					setType(rs.getInt("TYPE")).
					setSorting(rs.getString("ASC_OR_DESC")).
					setTableName(currentTableName);
			
			//MSSQL returns null at times
			if(tempCol.getColumnName() == null) {
				continue;
			}
			
			a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).put(tempCol.getColumnName(), tempCol);
			
			
//			System.out.println(rs.getString("COLUMN_NAME") + "-------------------------------->");
//			System.out.println("NON_UNIQUE > " + rs.getString("NON_UNIQUE"));
//			System.out.println("TYPE > " + rs.getString("TYPE"));
//			System.out.println("ASC_OR_DESC > " + rs.getString("ASC_OR_DESC"));
		}
		
		rs = a_dbCompareStructure.sourceDBMetaData.getImportedKeys(tempSchema, tempSchema, currentTableName);
		while(rs.next()) {
			if(a_dbCompareStructure.sourceTableColumnIndexes.containsKey(currentTableName) == false) {
				a_dbCompareStructure.sourceTableColumnIndexes.put(currentTableName, new TreeMap<String, DBColumnIndex>());
			}
			DBColumnIndex tempCol = new DBColumnIndex();
			
			if(a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).containsKey(rs.getString("FKCOLUMN_NAME"))) {
				tempCol = a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).get(rs.getString("FKCOLUMN_NAME"));
			}else {
				a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).put(rs.getString("FKCOLUMN_NAME"), tempCol);
				tempCol.setColumnName(rs.getString("FKCOLUMN_NAME")).
					setTableName(currentTableName);
			}
			
			tempCol.setPKTable(rs.getString("PKTABLE_NAME")).
				setPKColumn(rs.getString("PKCOLUMN_NAME")).
				setUpdateRule(rs.getInt("UPDATE_RULE")).
				setDeleteRule(rs.getInt("DELETE_RULE")).
				setDeferrability(rs.getInt("DEFERRABILITY"));
			
//			System.out.println(rs.getString("FKCOLUMN_NAME") + "-------------------------------->");
//			System.out.println("PKTABLE_NAME > " + rs.getString("PKTABLE_NAME"));
//			System.out.println("PKCOLUMN_NAME > " + rs.getString("PKCOLUMN_NAME"));
//			System.out.println("UPDATE_RULE > " + rs.getString("UPDATE_RULE"));
//			System.out.println("DELETE_RULE > " + rs.getString("DELETE_RULE"));
//			System.out.println("DEFERRABILITY > " + rs.getString("DEFERRABILITY"));
		}
//		System.out.println("===========================================================================\n\n");
		
		// working on target from here
		tempSchema = a_dbCompareStructure.targetConnection.getSchemaName();
		if (tempSchema != null && tempSchema.trim().length() < 1) {
			tempSchema = null;
		}
		rs = a_dbCompareStructure.targetDBMetaData.getIndexInfo(tempSchema,  tempSchema,  currentTableName, false, true);

		while(rs.next()) {
			
			if(a_dbCompareStructure.targetTableColumnIndexes.containsKey(currentTableName) == false) {
				a_dbCompareStructure.targetTableColumnIndexes.put(currentTableName, new TreeMap<String, DBColumnIndex>());
			}
			
			DBColumnIndex tempCol = new DBColumnIndex().
					setColumnName(rs.getString("COLUMN_NAME")).
					setNonUnique(rs.getBoolean("NON_UNIQUE")).
					setType(rs.getInt("TYPE")).
					setSorting(rs.getString("ASC_OR_DESC")).
					setTableName(currentTableName);
			
			if(tempCol.getColumnName() == null) {
				continue;
			}
			
			a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).put(tempCol.getColumnName(), tempCol);
			
			
//			System.out.println(rs.getString("COLUMN_NAME") + "-------------------------------->");
//			System.out.println("NON_UNIQUE > " + rs.getString("NON_UNIQUE"));
//			System.out.println("TYPE > " + rs.getString("TYPE"));
//			System.out.println("ASC_OR_DESC > " + rs.getString("ASC_OR_DESC"));
		}
		
		
		rs = a_dbCompareStructure.targetDBMetaData.getImportedKeys(tempSchema, tempSchema, currentTableName);
		while(rs.next()) {
			if(a_dbCompareStructure.targetTableColumnIndexes.containsKey(currentTableName) == false) {
				a_dbCompareStructure.targetTableColumnIndexes.put(currentTableName, new TreeMap<String, DBColumnIndex>());
			}
			DBColumnIndex tempCol = new DBColumnIndex();
			
			if(a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).containsKey(rs.getString("FKCOLUMN_NAME"))) {
				tempCol = a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).get(rs.getString("FKCOLUMN_NAME"));
			}else {
				a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).put(rs.getString("FKCOLUMN_NAME"), tempCol);
				tempCol.setColumnName(rs.getString("FKCOLUMN_NAME")).
					setTableName(currentTableName);
			}
			
			tempCol.setPKTable(rs.getString("PKTABLE_NAME")).
				setPKColumn(rs.getString("PKCOLUMN_NAME")).
				setUpdateRule(rs.getInt("UPDATE_RULE")).
				setDeleteRule(rs.getInt("DELETE_RULE")).
				setDeferrability(rs.getInt("DEFERRABILITY"));
			
//			System.out.println(rs.getString("FKCOLUMN_NAME") + "-------------------------------->");
//			System.out.println("PKTABLE_NAME > " + rs.getString("PKTABLE_NAME"));
//			System.out.println("PKCOLUMN_NAME > " + rs.getString("PKCOLUMN_NAME"));
//			System.out.println("UPDATE_RULE > " + rs.getString("UPDATE_RULE"));
//			System.out.println("DELETE_RULE > " + rs.getString("DELETE_RULE"));
//			System.out.println("DEFERRABILITY > " + rs.getString("DEFERRABILITY"));
		}
		
		if(a_dbCompareStructure.targetTableColumnIndexes.containsKey(currentTableName) == false || 
				a_dbCompareStructure.sourceTableColumnIndexes.containsKey(currentTableName) == false) {
			return isEqual;
		}
		
		for(DBColumnIndex tempColumn : a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).values()) {
			if(a_dbCompareStructure.sourceTableColumnIndexes.containsKey(currentTableName) == false) {
				return false;
			}
			
			if(a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).containsKey(tempColumn.getColumnName()) == false ||
					tempColumn.equals(a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).get(tempColumn.getColumnName())) == false) {
				isEqual = false;
			}
		}
		
		for(DBColumnIndex tempColumn : a_dbCompareStructure.sourceTableColumnIndexes.get(currentTableName).values()) {
			if(a_dbCompareStructure.targetTableColumnIndexes.containsKey(currentTableName) == false) {
				return false;
			}
			
			if(a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).containsKey(tempColumn.getColumnName()) == false ||
					tempColumn.equals(a_dbCompareStructure.targetTableColumnIndexes.get(currentTableName).get(tempColumn.getColumnName())) == false) {
				isEqual = false;
			}
		}
		
		return isEqual;
	}
	
	/**
	 * 
	 * @param a_strTableName need to pass this parameter only for MSSQL as they don't return the table name in 
	 * 			{@link ResultSetMetaData}
	 * @param sourceMetaData
	 * @param targetMetaData
	 * @param a_dbCompareStructure
	 * @return
	 * @throws Throwable
	 */
	public final static boolean compareDBTable(String a_strTableName, ResultSetMetaData sourceMetaData, ResultSetMetaData targetMetaData, 
			DBCompareStructure a_dbCompareStructure) throws Throwable {
		boolean isEqual = true;
		ResultSet rs = null;
		String currentTableName = a_strTableName;
		
		if(sourceMetaData == null || targetMetaData == null) {
			return false;
		}
		
		if(sourceMetaData.getColumnCount() != targetMetaData.getColumnCount()) {
			return false;
		}
		
		for(int iColCounter = 1; iColCounter <= sourceMetaData.getColumnCount(); iColCounter++) {
			if(iColCounter == 1) {
				a_dbCompareStructure.sourceTableColumns.put(currentTableName, new  TreeMap<String, DBColumn>());
			}
			
			DBColumn tempColumn = new DBColumn();
			tempColumn.setColumnName(sourceMetaData.getColumnName(iColCounter)).
				setColumnClassName(sourceMetaData.getColumnClassName(iColCounter)).
				setColumnType(sourceMetaData.getColumnType(iColCounter)).
				setTypeName(sourceMetaData.getColumnTypeName(iColCounter)).
				setPrecision(sourceMetaData.getPrecision(iColCounter)).
				setScale(sourceMetaData.getScale(iColCounter)).
				setReadOnly(sourceMetaData.isReadOnly(iColCounter)).
				setNullable(sourceMetaData.isNullable(iColCounter) == 1).
				setAutoIncreament(sourceMetaData.isAutoIncrement(iColCounter)).
				setTableName(currentTableName);
			
			a_dbCompareStructure.sourceTableColumns.get(tempColumn.getTableName()).put(tempColumn.getColumnName(), tempColumn);
		}
		String tempSchema = a_dbCompareStructure.sourceConnection.getSchemaName();
		if(tempSchema != null && tempSchema.trim().length() < 1) {
			tempSchema = null;
		}
		rs = a_dbCompareStructure.sourceDBMetaData.getPrimaryKeys(tempSchema, tempSchema, currentTableName);
		while(rs.next()) {
			String colName = rs.getString("COLUMN_NAME");
			if(a_dbCompareStructure.sourceTableColumns.get(currentTableName).containsKey(colName) == false) {
				System.out.println(colName + " not found below");
				System.out.println(a_dbCompareStructure.sourceTableColumns.get(currentTableName));
			}
			a_dbCompareStructure.sourceTableColumns.get(currentTableName).get(colName).setPrimaryKey(true);
		}
		
		for(int iColCounter = 1; iColCounter <= targetMetaData.getColumnCount(); iColCounter++) {
			if(iColCounter == 1) {
				a_dbCompareStructure.targetTableColumns.put(currentTableName, new  TreeMap<String, DBColumn>());
			}
			
			DBColumn tempColumn = new DBColumn();
			tempColumn.setColumnName(targetMetaData.getColumnName(iColCounter)).
				setColumnClassName(targetMetaData.getColumnClassName(iColCounter)).
				setColumnType(targetMetaData.getColumnType(iColCounter)).
				setTypeName(targetMetaData.getColumnTypeName(iColCounter)).
				setPrecision(targetMetaData.getPrecision(iColCounter)).
				setScale(targetMetaData.getScale(iColCounter)).
				setReadOnly(targetMetaData.isReadOnly(iColCounter)).
				setNullable(targetMetaData.isNullable(iColCounter) == 1).
				setAutoIncreament(targetMetaData.isAutoIncrement(iColCounter)).
				setTableName(currentTableName);
			
			a_dbCompareStructure.targetTableColumns.get(tempColumn.getTableName()).put(tempColumn.getColumnName(), tempColumn);
		}
		
		tempSchema = a_dbCompareStructure.targetConnection.getSchemaName();
		if (tempSchema != null && tempSchema.trim().length() < 1) {
			tempSchema = null;
		}
			
		rs = a_dbCompareStructure.targetDBMetaData.getPrimaryKeys(tempSchema, tempSchema, currentTableName);
		while(rs.next()) {
			String colName = rs.getString("COLUMN_NAME");
			a_dbCompareStructure.targetTableColumns.get(currentTableName).get(colName).setPrimaryKey(true);
		}
		
		for(DBColumn tempColumn : a_dbCompareStructure.targetTableColumns.get(currentTableName).values()) {
			if(a_dbCompareStructure.sourceTableColumns.containsKey(tempColumn.getTableName()) == false || 
					a_dbCompareStructure.sourceTableColumns.get(tempColumn.getTableName()).containsKey(tempColumn.getColumnName()) == false ||
					tempColumn.equals(a_dbCompareStructure.sourceTableColumns.get(tempColumn.getTableName()).get(tempColumn.getColumnName())) == false) {
				isEqual = false;
			}
		}
		
		return isEqual;
	}
}
