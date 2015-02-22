package edu.buffalo.cse562.schema;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class ColumnWithType extends Column {

	private String columnType;
	private int columnNumber;

	public ColumnWithType(Table table, String columnName, String columnType, int columnNumber) {
	
		super(table, columnName);
		this.columnType = columnType;
		this.columnNumber = columnNumber;
		
	}
	
	public ColumnWithType(Column col, String columnType, int columnNumber) {
		
		super(col.getTable(), col.getColumnName());
		this.columnType = columnType;
		this.columnNumber = columnNumber;
		
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public int getColumnNumber() {
		return columnNumber;
	}

	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}
	
	public Column getColumn() {
		return new Column(this.getTable(), this.getColumnName());
	}
}
