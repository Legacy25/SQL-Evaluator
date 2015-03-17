package edu.buffalo.cse562.schema;

import java.util.ArrayList;
import java.util.Arrays;

public class Schema {

	/*
	 * A class to encapsulate the Schema information of a relation.
	 * 
	 * tableName 	- The name of the table
	 * tableFile 	- In case the relation is a table on disk, this attribute
	 * 					contains the location and name of that file, otherwise
	 * 					it is set to "__mem__" indicating that it is a relation
	 * 					in memory
	 * 
	 * columns		- A list of the columns of this relation
	 */
	
	private String tableName;
	private String tableFile;
	private ArrayList<ColumnWithType> columns;
	
	
	public Schema(String tableName, String tableFile) {
		this.tableName = tableName;
		this.tableFile = tableFile;
		
		/* Initializations */
		columns = new ArrayList<ColumnWithType>();
	}
	
	public Schema(Schema schema) {
		this.tableName = schema.tableName;
		this.tableFile = schema.tableFile;
		this.columns = new ArrayList<ColumnWithType>();
		
		for(int i=0; i<schema.columns.size(); i++) {
			columns.add(new ColumnWithType(schema.columns.get(i)));
		}
	}
	
	public Schema() {
		this.tableName = "";
		this.tableFile = "IN-MEMORY";
		
		/* Initializations */
		columns = new ArrayList<ColumnWithType>();
	}

	
	
	/* Getters and Setters */
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
	
	
	
	@Override
	public String toString() {
		/* Return a formatted string containing the schema information */
		String result = "Schema for table " + tableName	+ "\nTable File: " + tableFile +"\nColumns:\n";
		
		for(ColumnWithType col:columns) {
			result = result + col.toString() + "\t";
		}
		
		result = result + "\n";
		return result;
	}
	
}
