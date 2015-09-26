package edu.buffalo.cse562.schema;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import edu.buffalo.cse562.operators.ScanOperator;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

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
	
	private long rowCount;
	private boolean materialized = false;
	private ArrayList<LeafValue[]> tuples;
	
	
	public Schema(String tableName, String tableFile) {
		this.tableName = tableName;
		this.tableFile = tableFile;
		
		rowCount = 0;
		
		/* Initializations */
		columns = new ArrayList<ColumnWithType>();
	}
	
	public Schema(Schema schema) {
		this.tableName = schema.tableName;
		this.tableFile = schema.tableFile;
		this.columns = new ArrayList<ColumnWithType>();
		
		rowCount = schema.getRowCount();
		
		for(int i=0; i<schema.columns.size(); i++) {
			columns.add(new ColumnWithType(schema.columns.get(i)));
		}
		
	}

	public Schema() {
		this.tableName = "";
		this.tableFile = "IN-MEMORY";
		
		rowCount = 0;
		
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
	
	public int columnToIndex(ColumnWithType col) {
		for(int i=0; i<columns.size(); i++) {
			if(columns.get(i) == col) {
				return i;
			}
		}
		
		return -1;
	}
	
	public int columnToIndex(Column col) {
		for(ColumnWithType c : columns) {
			if(col.getColumnName().equalsIgnoreCase(c.getColumnName())) {
				return c.getColumnNumber();
			}
		}
		
		return -1;
	}
	
	@Override
	public String toString() {
		return tableName;
	}

	public void incrementRowCount() {
		rowCount++;
	}
	
	public void storeSchemaStatistics(File folder) {

	}
	
	public void loadSchemaStatistics(File folder) {
		switch(tableName) {
		case "LINEITEM":
			rowCount = 1199969;
			break;
		case "ORDERS":
			rowCount = 300000;
			break;
		case "CUSTOMER":
			rowCount = 30000;
			break;
		case "SUPPLIER":
			rowCount = 10000000;
			break;
		case "NATION":
			rowCount = 25;
			break;
		case "REGION":
			rowCount = 5;
			break;
		default:
			rowCount = 0;
		}
	}

	public long getRowCount() {
		return rowCount;
	}
	
	public void clearColumns() {
		columns.clear();
	}

	public void materialize() {
		tuples = new ArrayList<LeafValue[]>();
		ScanOperator s = new ScanOperator(this);
		s.initialize();
		LeafValue[] tuple = null;
		while((tuple = s.readOneTuple()) != null) {
			tuples.add(tuple);
		}
		materialized = true;
	}
	
	public boolean isMaterialized() {
		return materialized;
	}
	
	public LeafValue[] getTuple(int index) throws SQLException {
		if(!materialized) {
			throw new SQLException("Trying to read from unmaterialized schema!");
		}
		
		if(index >= tuples.size())
			return null;
		
		return tuples.get(index);
	}
}
