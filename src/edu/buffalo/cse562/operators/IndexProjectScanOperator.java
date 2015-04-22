package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import edu.buffalo.cse562.QueryPreprocessor;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class IndexProjectScanOperator implements Operator {

	private Schema oldSchema;
	private Schema newSchema;
	private Expression where;
	private Column col;
	private String filePrefix;
	private ArrayList<LeafValue> literalValues;
	private ArrayList<String> searchKeys;
	private int keyIndex;
	
	private BufferedReader br;
	
	private boolean[] selectedCols;
	
	
	public IndexProjectScanOperator(Schema oldSchema, Schema newSchema, Expression where) {
		this.oldSchema = oldSchema;
		this.newSchema = newSchema;
		this.where = where;
		
		literalValues = new ArrayList<LeafValue>();
		searchKeys = new ArrayList<String>();
		
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
		
		keyIndex = 0;
		br = null;
		filePrefix = Main.indexDirectory+"/"
				+oldSchema.getTableName()+".Secondary."
				+col.getColumnName()+".";
		
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
			searchKeys.add(QueryPreprocessor.keyToFile(l));
		}
		
		try {
			br = new BufferedReader(new FileReader(new File(filePrefix+searchKeys.get(keyIndex)+".dat")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public LeafValue[] readOneTuple() {
		if(br == null) {
			return null;
		}

		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(line == null) {
			keyIndex++;
			
			if(keyIndex >= searchKeys.size()) {
				return null;
			}
			
			try {
				br = new BufferedReader(new FileReader(new File(filePrefix+searchKeys.get(keyIndex)+".dat")));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
			return readOneTuple();
		}
		
		return constructTuple(line);
	}
	
	public LeafValue[] constructTuple(String line) {
		/* Split the tuple into attributes using the '|' delimiter */
		String cols[] = line.split("\\|");
		
		/* LeafValue array that will hold the tuple to be returned */
		LeafValue ret[] = new LeafValue[newSchema.getColumns().size()];
		
		/* 
		 * Iterate over each column according to schema's type information
		 * and populate the ret array with appropriate value and LeafValue
		 * type
		 */
		int k = 0;
		for(int i=0; i<cols.length; i++) {
			if(selectedCols[i]) {
				String type = oldSchema.getColumns().get(i).getColumnType();
				
				switch(type) {
				case "int":
					ret[k] = new LongValue(cols[i]);
					break;
				
				case "decimal":
					ret[k] = new DoubleValue(cols[i]);
					break;
				
				case "char":
				case "varchar":
				case "string":
					/* Blank spaces are appended to account for JSQLParser's weirdness */
					ret[k] = new StringValue(" "+cols[i]+" ");
					break;

				case "date":
					/* Same deal as string */
					ret[k] = new DateValue(" "+cols[i]+" ");
					break;
				default:
					System.err.println("Unknown column type");
				}
				
				k++;
			}
		}
		
		/* Return the generated tuple */
		return ret;
	}
	
	private void close() {
		if(br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void reset() {
		
		close();
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
