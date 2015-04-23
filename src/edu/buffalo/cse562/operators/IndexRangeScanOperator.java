package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class IndexRangeScanOperator implements Operator {

	private Schema oldSchema, newSchema;
	private ArrayList<Expression> chosenList;
	
	private ArrayList<File> fileList;
	private String filePrefix;
	private Long low, high;
	private int fileIndex;
	
	private BufferedReader br;
	private boolean[] selectedCols;
	
	public IndexRangeScanOperator(Schema oldSchema, Schema newSchema,
			ArrayList<Expression> chosenList, Column chosenColumn) {
		
		this.oldSchema = oldSchema;
		this.newSchema = newSchema;
		this.chosenList = chosenList;
		
		filePrefix = oldSchema.getTableName()+
				".Secondary."+chosenColumn.getColumnName()+".";
		
		low = null;
		high = null;
		br = null;
		
		getLimits();
		
		selectedCols = new boolean[oldSchema.getColumns().size()];
		
		fileList = new ArrayList<File>();
		fileIndex = 0;
		
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

	private void getLimits() {
		for(Expression exp : chosenList) {
			BinaryExpression be = (BinaryExpression) exp;
			long bound = getBoundFromExpression(be.getRightExpression());
			if(be instanceof GreaterThanEquals || be instanceof GreaterThan) {
				low = bound;
			}
			else {
				high = bound;
			}
		}
		
		if(high == null) {
			high = Long.MAX_VALUE;
		}
		
		if(low == null) {
			low = Long.MIN_VALUE;
		}
	}

	@SuppressWarnings("deprecation")
	private long getBoundFromExpression(Expression rightExpression) {
		
		LeafValue lv = null;
		
		if(rightExpression instanceof Function) {
			String lvstr = (String) ((Function) rightExpression).getParameters().getExpressions().get(0).toString();
			lv = new DateValue(lvstr);
		}
		else {
			lv = (LeafValue) rightExpression;
		}
		
		if(lv instanceof LongValue) {
			return ((LongValue) lv).toLong() % 10000;
		}
		else if(lv instanceof DoubleValue) {
			return (long) (((DoubleValue) lv).toDouble() % 10000);
		}
		else if(lv instanceof DateValue) {
			return ((DateValue) lv).getValue().getYear();
		}
		
		return 0;
	}

	@Override
	public Schema getSchema() {
		return newSchema;
	}

	@Override
	public void generateSchemaName() {
		newSchema.setTableName("iRangeScan ("+ oldSchema.getTableName() +")");
	}

	@Override
	public void initialize() {
		
		for(File f : Main.indexDirectory.listFiles()) {
			String name = f.getName();
			if(name.startsWith(filePrefix)) {
				String[] fields = name.split("\\.");
				Long val = Long.parseLong(fields[3]);
				if(val <= high && val >= low) {
					fileList.add(new File(Main.indexDirectory+"/"+name));
				}
			}
		}
		
		if(fileList.size() > 0) {
			try {
				br = new BufferedReader(new FileReader(fileList.get(0)));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
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
			fileIndex++;
			
			if(fileIndex >= fileList.size()) {
				return null;
			}
			
			try {
				br = new BufferedReader(new FileReader(fileList.get(fileIndex)));
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

	@Override
	public void reset() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
