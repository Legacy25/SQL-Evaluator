package edu.buffalo.cse562.operators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class IndexProjectScanOperator implements Operator {

	private Schema oldSchema;
	private Schema newSchema;
	
	
	private HashSet<String> selectedColumnNames;
	private boolean[] selectedCols;
	
	
	private Environment db;
	private Database table;
	private Cursor cursor;
	
	
	public IndexProjectScanOperator(Schema schema, HashSet<String> selectedColumnNames) {
		this.oldSchema = schema;
		this.selectedColumnNames = selectedColumnNames;
		
		db = null;
		table = null;
		cursor = null;
		
		selectedCols = new boolean[schema.getColumns().size()];
		Arrays.fill(selectedCols, false);
		
		buildSchema();
	}
	
	private void buildSchema() {
		newSchema = new Schema(oldSchema.getTableName(), oldSchema.getTableFile());
		
		int i = 0;
		for(ColumnWithType c : oldSchema.getColumns()) {
			if(selectedColumnNames.contains(c.getColumnName().toLowerCase())) {
				newSchema.addColumn(c);
				selectedCols[i] = true;
			}
			
			i++;
		}
		
	}
	
	@Override
	public Schema getSchema() {
		return newSchema;
	}

	@Override
	public void generateSchemaName() {
		
	}

	@Override
	public void initialize() {
		
		try {
			db = new Environment(Main.indexDirectory, null);
			table = db.openDatabase(null, oldSchema.getTableName(), null);
			
			cursor = table.openCursor(null, null);
			
		} catch (DatabaseException e) {
			e.printStackTrace();
			if(cursor != null) {
				cursor.close();
			}
			if(table != null) {
				table.close();
			}
			if(db != null) {
				db.close();
			}
		} 
				
	}

	@Override
	public LeafValue[] readOneTuple() {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry val = new DatabaseEntry();
		
		try {
			if(cursor.getNext(key, val, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				ByteArrayInputStream in = new ByteArrayInputStream(val.getData());
				DataInputStream dis = new DataInputStream(in);
				
				LeafValue[] newTuple = new LeafValue[newSchema.getColumns().size()];
				
				int k = 0;
				for(int i=0; i<selectedCols.length; i++) {
					String sValue = null;
					Long lValue = null;
					Double dValue = null;
					String c = oldSchema.getColumns().get(i).getColumnType();
					
					switch(c) {
					case "int":
						lValue = dis.readLong();
						break;
					
					case "decimal":
						dValue = dis.readDouble();
						break;
					
					case "char":
					case "varchar":
					case "string":
					case "date":
						sValue = dis.readUTF();
						break;
					}
					if(selectedCols[i]) {
						switch(c) {
						case "int":
							newTuple[k] = new LongValue(lValue);
							break;
						
						case "decimal":
							newTuple[k] = new DoubleValue(dValue);
							break;
						
						case "char":
						case "varchar":
						case "string":
							newTuple[k] = new StringValue(sValue);
							break;
						case "date":
							newTuple[k] = new DateValue(sValue);
							break;
						}
						k++;
					}
				}
				
				return newTuple;
			}
			else {
				cursor.close();
				table.close();
				db.close();
				
				return null;
			}
		} catch(IOException e) {
			cursor.close();
			table.close();
			db.close();
			
			e.printStackTrace();
			return null;
		}
		
	}

	@Override
	public void reset() {
		
		initialize();
				
	}

	@Override
	public Operator getLeft() {
		return null;
	}

	@Override
	public Operator getRight() {
		return null;
	}

	@Override
	public void setLeft(Operator o) {
		
	}

	@Override
	public void setRight(Operator o) {
		
	}
}
