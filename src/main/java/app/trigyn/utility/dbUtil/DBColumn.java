package app.trigyn.utility.dbUtil;

import org.apache.commons.lang3.StringUtils;

public class DBColumn{
	
	
	private String	tableName			= null;
	private String	columnName			= null;
	private String	columnClassName		= null;
	private int		columnType			= -1;
	private String	typeName			= null;
	private int		precision			= -1;
	private int		scale				= -1;
	private boolean	isPrimaryKey		= false;
	private boolean	isNullable			= true;
	private boolean	isAutoIncreament	= false;
	private boolean	isReadOnly			= false;

	public String getColumnName() {
		return columnName;
	}

	public DBColumn setColumnName(String a_columnName) {
		columnName = a_columnName;
		return this;
	}

	public String getColumnClassName() {
		return columnClassName;
	}

	public DBColumn setColumnClassName(String a_columnClassName) {
		columnClassName = a_columnClassName;
		return this;
	}

	public int getColumnType() {
		return columnType;
	}

	public DBColumn setColumnType(int a_columnType) {
		columnType = a_columnType;
		return this;
	}

	public String getTypeName() {
		return typeName;
	}

	public DBColumn setTypeName(String a_typeName) {
		typeName = a_typeName;
		return this;
	}

	public int getPrecision() {
		return precision;
	}

	public DBColumn setPrecision(int a_precision) {
		precision = a_precision;
		return this;
	}

	public int getScale() {
		return scale;
	}

	public DBColumn setScale(int a_scale) {
		scale = a_scale;
		return this;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public DBColumn setPrimaryKey(boolean a_isPrimaryKey) {
		isPrimaryKey = a_isPrimaryKey;
		return this;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public DBColumn setNullable(boolean a_isNullable) {
		isNullable = a_isNullable;
		return this;
	}

	public boolean isAutoIncreament() {
		return isAutoIncreament;
	}

	public DBColumn setAutoIncreament(boolean a_isAutoIncreament) {
		isAutoIncreament = a_isAutoIncreament;
		return this;
	}

	public boolean isReadOnly() {
		return isReadOnly;
	}

	public DBColumn setReadOnly(boolean a_isReadOnly) {
		isReadOnly = a_isReadOnly;
		return this;
	}
	
	@Override
	public boolean equals(Object a_obj) {
		if(a_obj == null || a_obj instanceof DBColumn == false) {
			return false;
		}
		
		DBColumn comparable = (DBColumn)a_obj;
		if(StringUtils.equals(comparable.columnName, columnName) == false) {
			return false;
		}
		
		if(StringUtils.equals(comparable.columnClassName, columnClassName) == false) {
			return false;
		}
		
		if(StringUtils.equals(comparable.typeName, typeName) == false) {
			return false;
		}
		
		if(comparable.precision != precision) {
			return false;
		}

		if(comparable.scale != scale) {
			return false;
		}

		if(comparable.isNullable != isNullable) {
			return false;
		}

		if(comparable.isAutoIncreament != isAutoIncreament) {
			return false;
		}
		
		if(comparable.isReadOnly != isReadOnly) {
			return false;
		}
		
		if(comparable.isPrimaryKey != isPrimaryKey) {
			return false;
		}

		return true;
	}

	public String getTableName() {
		return tableName;
	}

	public DBColumn setTableName(String a_tableName) {
		tableName = a_tableName;
		return this;
	}
}