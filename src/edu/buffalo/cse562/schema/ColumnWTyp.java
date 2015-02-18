package edu.buffalo.cse562.schema;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class ColumnWTyp extends Column {

	private String columnType;

	public ColumnWTyp(Table table, String columnName, String columnType) {
	
		super(table, columnName);
		this.columnType = columnType;
		
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
}
