package edu.buffalo.cse562.schema;

import java.util.ArrayList;
import java.util.Arrays;

public class Schema {

	private String tableName;
	private String tableFile;
	private ArrayList<ColumnWithType> columns;
	
	public Schema(String tableName, String tableFile) {
		this.tableName = tableName;
		this.tableFile = tableFile;
		columns = new ArrayList<ColumnWithType>();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableFile() {
		return tableFile;
	}

	public void setTableFile(String tableFile) {
		this.tableFile = tableFile;
	}

	public ArrayList<ColumnWithType> getColumns() {
		return columns;
	}

	public void addColumn(ColumnWithType column) {
		this.columns.add(column);
	}
	
	public void addColumns(ColumnWithType columns[]) {
		this.columns.addAll(Arrays.asList(columns));
	}
	
	public String toString() {
		
		return "Schema for table " + tableName
				+ "\nTable File: " + tableFile 
				+ "\nColumns: " + columns;
	}
	
}
