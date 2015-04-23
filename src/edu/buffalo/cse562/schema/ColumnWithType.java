package edu.buffalo.cse562.schema;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class ColumnWithType extends Column {

	/*
	 * An augmentation of the Column class to include
	 * type information and the attribute order information
	 */
	
	private String columnType;
	private int columnNumber;
	private int size;

	
	public ColumnWithType(Table table, String columnName, String columnType, int columnNumber) {
		super(table, columnName);
		this.columnType = columnType;
		this.columnNumber = columnNumber;
		
		if(columnType == null) {
			size = 0;
			return;
		}
		
		switch(columnType) {
		case "int":
			size = 10;
			break;
		case "decimal":
			size = 15;
			break;
		
		case "char":
		case "varchar":
		case "string":
			System.err.println("Must specify string length!");
			break;

		case "date":
			size = 10;
			break;
		default:
			System.err.println("Unknown column type");
		}
	}
	
	public ColumnWithType(Table table, String columnName,
			String columnType, int columnNumber, int size) {
		super(table, columnName);
		this.columnType = columnType;
		this.columnNumber = columnNumber;
		this.size = size;
	}
	
	public ColumnWithType(Column col, String columnType, int columnNumber) {
		super(col.getTable(), col.getColumnName());
		this.columnType = columnType;
		this.columnNumber = columnNumber;
		if(columnType == null) {
			size = 0;
			return;
		}
		
		switch(columnType) {
		case "int":
			size = 7;
			break;
		case "decimal":
			size = 9;
			break;
		
		case "char":
		case "varchar":
		case "string":
			System.err.println("Must specify string length!");
			break;

		case "date":
			size = 10;
			break;
		default:
			System.err.println("Unknown column type");
		}
	}

	public ColumnWithType(ColumnWithType col) {
		super(col.getTable(), col.getColumnName());
		this.columnType = col.columnType;
		this.columnNumber = col.columnNumber;
		this.size = col.size;
	}
	
	/* Getters and Setters */
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
	
	public int getSize() {
		return size;
	}
	
	
	@Override
	public String toString() {
		return this.getColumn().getWholeColumnName();
	}
}
