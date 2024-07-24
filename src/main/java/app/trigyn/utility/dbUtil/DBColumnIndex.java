package app.trigyn.utility.dbUtil;

import org.apache.commons.lang3.StringUtils;

public class DBColumnIndex {
	
	private String	tableName		= null;
	private String	columnName		= null;

	private boolean	isIndex			= false;
	private boolean	nonUnique		= true;
	private int		type			= -1;
	private String	sorting			= null;

	private boolean	isForeignKey	= false;
	private String	PKTable			= null;
	private String	PKColumn		= null;
	private int		updateRule		= -1;
	private int		deleteRule		= -1;
	private int		deferrability	= -1;

	public String getTableName() {
		return tableName;
	}

	public DBColumnIndex setTableName(String a_tableName) {
		tableName = a_tableName;
		return this;
	}

	public String getColumnName() {
		return columnName;
	}

	public DBColumnIndex setColumnName(String a_columnName) {
		columnName = a_columnName;
		return this;
	}

	public boolean isIndex() {
		return isIndex;
	}

	public DBColumnIndex setIndex(boolean a_isIndex) {
		isIndex = a_isIndex;
		return this;
	}

	public boolean isNonUnique() {
		return nonUnique;
	}

	public DBColumnIndex setNonUnique(boolean a_nonUnique) {
		nonUnique = a_nonUnique;
		return this;
	}

	public int getType() {
		return type;
	}

	public DBColumnIndex setType(int a_type) {
		type = a_type;
		return this;
	}

	public String getSorting() {
		return sorting;
	}

	public DBColumnIndex setSorting(String a_sorting) {
		sorting = a_sorting;
		return this;
	}

	public boolean isForeignKey() {
		return isForeignKey;
	}

	public DBColumnIndex setForeignKey(boolean a_isForeignKey) {
		isForeignKey = a_isForeignKey;
		return this;
	}

	public String getPKTable() {
		return PKTable;
	}

	public DBColumnIndex setPKTable(String a_pKTable) {
		PKTable = a_pKTable;
		return this;
	}

	public String getPKColumn() {
		return PKColumn;
	}

	public DBColumnIndex setPKColumn(String a_pKColumn) {
		PKColumn = a_pKColumn;
		return this;
	}

	public int getUpdateRule() {
		return updateRule;
	}

	public DBColumnIndex setUpdateRule(int a_updateRule) {
		updateRule = a_updateRule;
		return this;
	}

	public int getDeleteRule() {
		return deleteRule;
	}

	public DBColumnIndex setDeleteRule(int a_deleteRule) {
		deleteRule = a_deleteRule;
		return this;
	}

	public int getDeferrability() {
		return deferrability;
	}

	public DBColumnIndex setDeferrability(int a_deferrability) {
		deferrability = a_deferrability;
		return this;
	}
	
	@Override
	public boolean equals(Object a_obj) {
		if(a_obj == null || a_obj instanceof DBColumnIndex == false) {
			return false;
		}
		
		DBColumnIndex comparable = (DBColumnIndex)a_obj;
		if(StringUtils.equals(comparable.columnName, columnName) == false) {
			return false;
		}
		
		if(comparable.isIndex != isIndex) {
			return false;
		}
		
		if(comparable.nonUnique != nonUnique) {
			return false;
		}
		
		if(comparable.type != type) {
			return false;
		}
		
		if(StringUtils.equals(comparable.sorting, sorting) == false) {
			return false;
		}
		
		if(comparable.isForeignKey != isForeignKey) {
			return false;
		}
		
		if(StringUtils.equals(comparable.PKTable, PKTable) == false) {
			return false;
		}
		
		if(StringUtils.equals(comparable.PKColumn, PKColumn) == false) {
			return false;
		}
		
		if(comparable.updateRule != updateRule) {
			return false;
		}
		
		if(comparable.deleteRule != deleteRule) {
			return false;
		}
		
		if(comparable.deferrability != deferrability) {
			return false;
		}
		
		return true;
	}
}
