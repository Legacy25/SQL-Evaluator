package edu.buffalo.cse562.schema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import edu.buffalo.cse562.Main;

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
	private ArrayList<ColumnWithType> primaryKey;
	private ArrayList<ColumnWithType> foreignKeys;
	private ArrayList<ColumnWithType> secondaryIndexes;
	
	private long rowCount;
	
	
	public Schema(String tableName, String tableFile) {
		this.tableName = tableName;
		this.tableFile = tableFile;
		
		rowCount = 0;
		
		/* Initializations */
		columns = new ArrayList<ColumnWithType>();
		primaryKey = new ArrayList<ColumnWithType>();
		foreignKeys = new ArrayList<ColumnWithType>();
		secondaryIndexes = new ArrayList<ColumnWithType>();
	}
	
	public Schema(Schema schema) {
		this.tableName = schema.tableName;
		this.tableFile = schema.tableFile;
		this.columns = new ArrayList<ColumnWithType>();
		this.primaryKey = new ArrayList<ColumnWithType>();
		this.foreignKeys = new ArrayList<ColumnWithType>();
		this.secondaryIndexes = new ArrayList<ColumnWithType>();
		
		rowCount = schema.getRowCount();
		
		for(int i=0; i<schema.columns.size(); i++) {
			columns.add(new ColumnWithType(schema.columns.get(i)));
		}
		
		for(int i=0; i<schema.primaryKey.size(); i++) {
			primaryKey.add(new ColumnWithType(schema.primaryKey.get(i)));
		}
		
		for(ColumnWithType col : schema.getForeignKeys()) {
			foreignKeys.add(col);
		}
		
		for(ColumnWithType col : schema.getSecondaryIndexes()) {
			secondaryIndexes.add(col);
		}
	}

	public Schema() {
		this.tableName = "";
		this.tableFile = "IN-MEMORY";
		
		rowCount = 0;
		
		/* Initializations */
		columns = new ArrayList<ColumnWithType>();
		primaryKey = new ArrayList<ColumnWithType>();
		foreignKeys = new ArrayList<ColumnWithType>();
		secondaryIndexes = new ArrayList<ColumnWithType>();
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
	
	public ColumnWithType getPrimaryKey(int i) {
		return primaryKey.get(i);
	}
	
	public ArrayList<ColumnWithType> getPrimaryKey() {
		return primaryKey;
	}
	
	public int getPrimaryKeySize() {
		return primaryKey.size();
	}
	
	public void addToPrimaryKey(ColumnWithType column) {
		this.primaryKey.add(column);
	}
	
	public ArrayList<ColumnWithType> getForeignKeys() {
		return foreignKeys;
	}
	
	public void addToForeignKeys(ColumnWithType column) {
		this.foreignKeys.add(column);
	}
	
	public ArrayList<ColumnWithType> getSecondaryIndexes() {
		return secondaryIndexes;
	}
	
	public void addToSecondaryIndexes(ColumnWithType col) {
		secondaryIndexes.add(col);
	}
	
	public int columnToIndex(ColumnWithType col) {
		for(int i=0; i<columns.size(); i++) {
			if(columns.get(i) == col) {
				return i;
			}
		}
		
		return -1;
	}
	
	
	@Override
	public String toString() {
		/* Return a formatted string containing the schema information */
		String result = tableName+"\n";
		
		for(ColumnWithType col:columns) {
			if(foreignKeys.contains(col)) {
				result = result + col.toString() + "(sInd) | ";
			}
			else if (primaryKey.contains(col)) {
				result = result + col.toString() + "(pK) | ";
			}
			else {
				result = result + col.toString() + " | ";
			}
		}
		
		result = result.substring(0, result.length() - 2) + "\n";
		return result;
	}

	public void incrementRowCount() {
		rowCount++;
	}
	
	public void storeSchemaStatistics(File folder) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(folder+"/"+tableName, false));
			bw.write(rowCount+"\n");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void loadSchemaStatistics(File folder) {
//		try {
//			BufferedReader br = new BufferedReader(new FileReader(folder+"/"+tableName));
//			rowCount = Long.parseLong(br.readLine());
//			if(tableName.equalsIgnoreCase("SUPPLIER")) {
//				rowCount = 10000000;
//			}
//			br.close();
//		} catch (NumberFormatException e) {
//			e.printStackTrace();
//		} catch (FileNotFoundException e) {
//			if(Main.DEBUG) {
//				System.err.println("Statistics file not found");
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		switch(tableName) {
		case "LINEITEM":
			rowCount = 3000000;
			break;
		case "ORDERS":
			rowCount = 300000;
			break;
		case "CUSTOMER":
			rowCount = 30000;
			break;
		case "SUPPLIER":
			rowCount = 2000;
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
}
