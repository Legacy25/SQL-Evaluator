package edu.buffalo.cse562.operators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class IndexProjectScanOperator implements Operator {

	private Schema oldSchema;
	private Schema newSchema;
	private Expression where;
	
	private boolean[] selectedCols;
	
	
	private Environment db;
	private Database table;
	private DiskOrderedCursor cursor;
	
	
	public IndexProjectScanOperator(Schema oldSchema, Schema newSchema, Expression where) {
		this.oldSchema = oldSchema;
		this.newSchema = newSchema;
		this.where = where;
		
		db = null;
		table = null;
		cursor = null;
		
		selectedCols = new boolean[oldSchema.getColumns().size()];
		Arrays.fill(selectedCols, false);
		
		buildSchema();
	}
	
	private void buildSchema() {
		generateSchemaName();
		
		int i = 0;
		for(ColumnWithType c : oldSchema.getColumns()) {
			for(int j=0; j<newSchema.getColumns().size(); j++) {
				if(c.getColumnName().equalsIgnoreCase(newSchema.getColumns().get(j).getColumnName())) {
					selectedCols[i] = true;
				}
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
		newSchema.setTableName("iScan {" + where + "} ("+oldSchema.getTableName()+")");
	}

	@Override
	public void initialize() {
		
		try {

			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setLocking(false);
			envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
			envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
			
			DatabaseConfig dbConfig = new DatabaseConfig();
			if(oldSchema.getTableName().equalsIgnoreCase("LINEITEM")) {
				dbConfig.setSortedDuplicates(true);
			}

			db = new Environment(Main.indexDirectory, envConfig);
			table = db.openDatabase(null, oldSchema.getTableName(), dbConfig);
			
			DiskOrderedCursorConfig curConfig = new DiskOrderedCursorConfig();
			curConfig.setQueueSize(100000);
			cursor = table.openCursor(curConfig);
			
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
			if(cursor.getNext(key, val, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
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
