package edu.buffalo.cse562.schema;

public class Column {

	private String columnName;
	private String columnType;
	
	public Column(String columnName, String columnType) {
		this.columnName = columnName;
		this.columnType = columnType;
	}
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnType() {
		return columnType;
	}
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	
}
