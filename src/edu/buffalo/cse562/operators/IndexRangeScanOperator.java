package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
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
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.schema.ColumnInfo;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class IndexRangeScanOperator extends Eval implements Operator {

	private Schema oldSchema, newSchema;
	private ArrayList<Expression> chosenList;
	
	private ArrayList<File> fileList, selectionFileList;
	private String filePrefix;
	private Long[] low, high;
	private int fileIndex;
	private int size;
	
	private BufferedReader br;
	private ArrayList<Integer> selectedCols;
	private boolean selectionMode;
	
	private LeafValue[] tuple;
	private HashMap<Column, ColumnInfo> TypeCache;
	
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
		
		selectionMode = true;
		
		size = newSchema.getColumns().size();
		getLimits();
		
		selectedCols = new ArrayList<Integer>();
		
		fileList = new ArrayList<File>();
		selectionFileList = new ArrayList<File>();
		fileIndex = 0;
		TypeCache = new HashMap<Column, ColumnInfo>();
		
		buildSchema();
	}

	private void buildSchema() {
		generateSchemaName();
		
		int i = 0;
		for(ColumnWithType c : oldSchema.getColumns()) {
			for(int j=0; j<newSchema.getColumns().size(); j++) {
				if(c.getColumnName().equalsIgnoreCase(newSchema.getColumns().get(j).getColumnName())) {
					selectedCols.add(i);
				}
			}
			
			i++;
		}
		
	}

	private void getLimits() {
		for(Expression exp : chosenList) {
			BinaryExpression be = (BinaryExpression) exp;
			Long[] bound = getBoundFromExpression(be.getRightExpression());
			if(be instanceof GreaterThanEquals || be instanceof GreaterThan) {
				low = bound;
			}
			else {
				high = bound;
			}
		}
		
		if(high == null) {
			high = new Long[]{Long.MAX_VALUE, (long) 11};
		}
		
		if(low == null) {
			low = new Long[]{Long.MIN_VALUE, (long) 0};
		}
	}

	@SuppressWarnings("deprecation")
	private Long[] getBoundFromExpression(Expression rightExpression) {
		
		LeafValue lv = null;
		
		if(rightExpression instanceof Function) {
			String lvstr = (String) ((Function) rightExpression).getParameters().getExpressions().get(0).toString();
			lv = new DateValue(lvstr);
		}
		else {
			lv = (LeafValue) rightExpression;
		}
		
		if(lv instanceof LongValue) {
			return new Long[]{((LongValue) lv).toLong() % 10000, null};
		}
		else if(lv instanceof DoubleValue) {
			return new Long[]{(long) (((DoubleValue) lv).toDouble() % 10000), null};
		}
		else if(lv instanceof DateValue) {
			return new Long[]{new Long(((DateValue) lv).getValue().getYear()), 
				new Long(((DateValue) lv).getValue().getMonth())
			};
		}
		
		return null;
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
				String[] vals = fields[3].split("\\-");
				Long[] val = new Long[2];
				val[0] = Long.parseLong(vals[0]);
				if(vals.length == 2) {
					val[1] = Long.parseLong(vals[1]);
				}
				else {
					val[1] = null;
				}
				if(val[1] == null) {
					if(val[0] <= high[0] && val[0] >= low[0]) {
						fileList.add(new File(Main.indexDirectory+"/"+name));
					}
				}
				else {
					if(val[0] < high[0] && val[0] > low[0]) {
						fileList.add(new File(Main.indexDirectory+"/"+name));
					}
					else if(val[0] > high[0] || val[0] < low[0]) {
						continue;
					}
					else if(val[0].longValue() == high[0].longValue()
							&& val[1].longValue() == high[1].longValue()) {
						selectionFileList.add(new File(Main.indexDirectory+"/"+name));
					}
					else if(val[0].longValue() == low[0].longValue() 
							&& val[1].longValue() == low[1].longValue()) {
						selectionFileList.add(new File(Main.indexDirectory+"/"+name));
					}
					else if(val[0].longValue() == high[0].longValue() && val[1] <= high[1]) {
						fileList.add(new File(Main.indexDirectory+"/"+name));
					}
					else if(val[0].longValue() == low[0].longValue() && val[1] >= low[1]) {
						fileList.add(new File(Main.indexDirectory+"/"+name));
					}
				}
			}
		}
		
		if(selectionFileList.size() > 0) {
			selectionMode = true;
			
		}
	}

	@Override
	public LeafValue[] readOneTuple() {
		
		if(br == null) {
			if(selectionMode) {
				if(fileIndex == selectionFileList.size()) {
					selectionMode = false;
					fileIndex = 0;
					return readOneTuple();
				}
				
				try {
					br = new BufferedReader(new FileReader(selectionFileList.get(fileIndex)));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				fileIndex++;
			}
			else {
				if(fileIndex == fileList.size()) {
					return null;
				}
				
				try {
					br = new BufferedReader(new FileReader(fileList.get(fileIndex)));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				fileIndex++;
			}
		}
		
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(line == null) {
			br = null;
			return readOneTuple();
		}
		
		if(selectionMode) {
			while(true) {
				tuple = constructTuple(line);
				if(satisfiesConditions(tuple)) {
					return tuple;
				}
				try {
					line = br.readLine();
					if(line == null) {
						break;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			br = null;
			return readOneTuple();
		}
		else {
			return constructTuple(line);
		}
	}
	
	private boolean satisfiesConditions(LeafValue[] tuple) {

		BooleanValue test = null;
		
		for(int i=0; i<chosenList.size(); i++) {
			try {
				test = (BooleanValue) eval(chosenList.get(i));
			} catch (SQLException e) {
				e.printStackTrace();
			}
			if(!test.getValue())
				break;
		}
		
		return test.getValue();
	}

	private LeafValue[] constructTuple(String line) {
		/* Split the tuple into attributes using the '|' delimiter */
		String cols[] = line.split("\\|");	
		
		/* LeafValue array that will hold the tuple to be returned */
		LeafValue ret[] = new LeafValue[size];
		
		/* 
		 * Iterate over each column according to schema's type information
		 * and populate the ret array with appropriate value and LeafValue
		 * type
		 */
		int k = 0;
		for(int i:selectedCols) {
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
				ret[k] = new StringValue(" "+cols[i]+" ");
				break;
			default:
				System.err.println("Unknown column type");
			}
			
			k++;
		}
		
		/* Return the generated tuple */
		return ret;
	}

	@Override
	public void reset() {
		if(br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
	
	
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		
		/* Necessary initializations */
		LeafValue lv = null;
		String type = null;
		int pos = 0;
		
		
		if(TypeCache.containsKey(arg0)) {
			/*
			 * Cache already contains the information, just need
			 * to locate it
			 */
			type = TypeCache.get(arg0).type;
			pos = TypeCache.get(arg0).pos;
		}
		else {
			/*
			 * This will happen for the first tuple only,
			 * we generate the ColumnInfo for this Column
			 * and store it in TypeCache, so that we do not
			 * need to go through this for every subsequent
			 * tuple
			 */
			for(int i=0; i<newSchema.getColumns().size(); i++) {
				/* 
				 * Loop over all columns and 
				 * try to find a column with
				 * the same name as arg0
				 */
				if(arg0.getWholeColumnName().equalsIgnoreCase(newSchema.getColumns().get(i).getWholeColumnName().toString())
						|| arg0.getWholeColumnName().equalsIgnoreCase(newSchema.getColumns().get(i).getColumnName().toString())) {
					type = newSchema.getColumns().get(i).getColumnType();
					pos = i;
					TypeCache.put(arg0, new ColumnInfo(type, pos));
					break;
				}
			}
		}
		
		switch(type) {
		case "int":
			lv = (LongValue) tuple[pos];
			break;
		case "decimal":
			lv = (DoubleValue) tuple[pos];
			break;
		case "char":
		case "varchar":
		case "string":
			lv = (StringValue) tuple[pos];
			break;
		case "date":
			lv = new DateValue(tuple[pos].toString());
			break;
		default:
			throw new SQLException();
		}		
		return lv;
	}

}
