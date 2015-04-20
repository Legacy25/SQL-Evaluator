package edu.buffalo.cse562.operators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.ParseTreeOptimizer;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class IndexProjectScanOperator implements Operator {

	private Schema oldSchema;
	private Schema newSchema;
	private Expression where;
	private Column col;
	private ArrayList<LeafValue> literalValues;
	private ArrayList<DatabaseEntry> searchKeys;
	private int keyIndex;
	private OperationStatus status;
	private DatabaseEntry key;
	private DatabaseEntry val;
	
	
	private boolean[] selectedCols;
	
	
	private Environment db;
	private Database table;
	private SecondaryDatabase index;
	private SecondaryCursor sCursor;
	
	
	public IndexProjectScanOperator(Schema oldSchema, Schema newSchema, Expression where) {
		this.oldSchema = oldSchema;
		this.newSchema = newSchema;
		this.where = where;
		
		db = null;
		table = null;
		sCursor = null;
		key = null;
		val = null;
		
		literalValues = new ArrayList<LeafValue>();
		searchKeys = new ArrayList<DatabaseEntry>();
		
		if(where instanceof Parenthesis) {
			Expression e = ((Parenthesis) where).getExpression();
			if(e instanceof OrExpression) {
				for(Expression exp : ParseTreeOptimizer.splitOrClauses(e)) {
					BinaryExpression be = (BinaryExpression) exp;
					col = (Column) be.getLeftExpression();
					literalValues.add((LeafValue) be.getRightExpression());
				}
			}
		}
		else {
			BinaryExpression be = (BinaryExpression) where;
			col = (Column) be.getLeftExpression();
			literalValues.add((LeafValue) be.getRightExpression());
		}
		
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

	@SuppressWarnings("deprecation")
	@Override
	public void initialize() {
		
		for(LeafValue l : literalValues) {
			searchKeys.add(getSearchKey(l));
		}
		
		keyIndex = 0;
		key = searchKeys.get(0);
		val = new DatabaseEntry();
		
		try {

			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setLocking(false);
			envConfig.setCacheSize(893386752);
			
			DatabaseConfig dbCon = new DatabaseConfig();
			dbCon.setReadOnly(true);

			db = new Environment(Main.indexDirectory, envConfig);
			table = db.openDatabase(null, oldSchema.getTableName(), dbCon);
			
				
			SecondaryConfig sCon = new SecondaryConfig();
			sCon.setSortedDuplicates(true);
			
			CursorConfig sCurCon = new CursorConfig();
			sCurCon.setReadUncommitted(true);
			
			index = db.openSecondaryDatabase(null, col.getColumnName(), table, sCon);
			sCursor = index.openSecondaryCursor(null, sCurCon);
			

			status = sCursor.getSearchKey(key, val, LockMode.READ_UNCOMMITTED);
			
		} catch (DatabaseException e) {
			e.printStackTrace();
			closeAll();
		} 
				
	}

	@Override
	public LeafValue[] readOneTuple() {
		
		try {
			if(status == OperationStatus.SUCCESS) {
				LeafValue[] output = constructTuple(val);
				status = sCursor.getNextDup(key, val, LockMode.READ_UNCOMMITTED);
				return output;
			}
			else {
				while(true) {
					keyIndex++;
					if(keyIndex >= searchKeys.size()) {
						break;
					}
					
					key = searchKeys.get(keyIndex);
					status = sCursor.getSearchKey(key, val, LockMode.READ_UNCOMMITTED);
					if(status == OperationStatus.SUCCESS) {
						return readOneTuple();
					}
				}
			}

			closeAll();
			return null;
		} catch(IOException e) {
			e.printStackTrace();
			
			closeAll();
			return null;
		}
	}
	
	private void closeAll() {
		if(sCursor != null) {
			sCursor.close();
		}
		if(index != null) {
			index.close();
		}
		if(table != null) {
			table.close();
		}
		if(db != null) {
			db.close();
		}
		
	}

	private LeafValue[] constructTuple(DatabaseEntry val) throws IOException {
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

	@Override
	public void reset() {
		
		closeAll();
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
	
	
	private DatabaseEntry getSearchKey(LeafValue l) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		
		try {
			if(l instanceof LongValue) {
				dos.writeLong(l.toLong());
			}
			else if (l instanceof DoubleValue) {
				dos.writeDouble(l.toDouble());
			}
			else if (l instanceof DateValue || l instanceof StringValue) {
				dos.writeUTF(l.toString().substring(1, l.toString().length()-1));
			}
		} catch (NumberFormatException | IOException | InvalidLeaf e) {
			e.printStackTrace();
		}
		
		DatabaseEntry key = new DatabaseEntry();
		key.setData(out.toByteArray());
		return key;
	}
}
