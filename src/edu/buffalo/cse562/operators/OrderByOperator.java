package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.schema.ColumnInfo;
import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class OrderByOperator extends Eval implements Operator {

	private Schema schema;
	private LeafValue[] next;
	private ArrayList<LeafValue[]> tempList;
	private int index;
	private int column;
	private Operator child;
	private HashMap<Column, ColumnInfo> TypeCache;


	public OrderByOperator(Expression expr, Operator child) {
		this.child = child;
		
		TypeCache = new HashMap<Column, ColumnInfo>();
		tempList = new ArrayList<LeafValue[]>();
		
		schema = child.getSchema();
		
		index = 0;
		
		column = 0;		
		findColumn(expr.toString());
		
		sort();

	}
	
	
	private void findColumn(String columnName) {
		
		for(int i=0; i<schema.getColumns().size(); i++) {
			if(schema.getColumns().get(i).getColumnName().equalsIgnoreCase(columnName)
					|| schema.getColumns().get(i).getWholeColumnName().equalsIgnoreCase(columnName)) {
				
				column = i;
				break;
				
			}
		}
		
	}
	
	

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public LeafValue[] readOneTuple() {
		
		
		if(index < tempList.size()) {
			LeafValue[] ret = tempList.get(index);
			index++;
			return ret;
		}

		
		return null;
		
		
	}

	@Override
	public void reset() {
		index = 0;
		child.reset();

	}

	
	private void sort() {
		
		LeafValue[] next = child.readOneTuple();
		
		while (next != null) {
			tempList.add(next);
			next = child.readOneTuple();
		}
		
		tempList = quicksort(tempList);

		
		child.reset();
	
	}
	
	
	
	
	

	private ArrayList<LeafValue[]> quicksort(ArrayList<LeafValue[]> tempList) {
		
		ArrayList<LeafValue[]> smaller = new ArrayList<LeafValue[]>();
		ArrayList<LeafValue[]> greater = new ArrayList<LeafValue[]>();
		
		if(tempList.size() <= 1)
			return tempList;
		
		String type = null;
		LeafValue pivot = tempList.get(0)[column];
		
		if(pivot instanceof LongValue) {
			type = "int";
		}
		else if(pivot instanceof DoubleValue) {
			type = "double";
		}
		else if(pivot instanceof StringValue) {
			type = "string";
		}
		else if(pivot instanceof DateValue) {
			type = "date";
		}
		
		for(int i=1; i<tempList.size(); i++) {
			try {
				switch(type) {
				case "int":
					if(tempList.get(i)[column].toLong() <= pivot.toLong()) {
						smaller.add(tempList.get(i));
					}
					else {
						greater.add(tempList.get(i));
					}
					break;
					
				case "decimal":
					if(tempList.get(i)[column].toDouble() <= pivot.toDouble()) {
						smaller.add(tempList.get(i));
					}
					else {
						greater.add(tempList.get(i));
					}
					break;
					
				case "string":
					if(tempList.get(i)[column].toString().compareTo(pivot.toString()) <= 0) {
						smaller.add(tempList.get(i));
					}
					else {
						greater.add(tempList.get(i));
					}
					break;
					
				case "date":
					if(tempList.get(i)[column].toString().compareTo(pivot.toString()) <= 0) {
						smaller.add(tempList.get(i));
					}
					else {
						greater.add(tempList.get(i));
					}
					break;
				
				}
					
			} catch (InvalidLeaf e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		ArrayList<LeafValue[]> sorted = new ArrayList<LeafValue[]>();
		sorted.addAll(quicksort(smaller));
		sorted.add(tempList.get(0));
		sorted.addAll(quicksort(greater));
		
		return sorted;

	}

	
	
	
	
	
	
	
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		
		LeafValue lv = null;
		String type = null;
		int pos = 0;
		
		if(TypeCache.containsKey(arg0)) {

			type = TypeCache.get(arg0).type;
			pos = TypeCache.get(arg0).pos;
		}
		else {

			for(int i=0; i<schema.getColumns().size(); i++) {
				
				if(arg0.getWholeColumnName().equalsIgnoreCase(schema.getColumns().get(i).getWholeColumnName().toString())
						|| arg0.getWholeColumnName().equalsIgnoreCase(schema.getColumns().get(i).getColumnName().toString())) {
					type = schema.getColumns().get(i).getColumnType();
					pos = i;
					TypeCache.put(arg0, new ColumnInfo(type, pos));
					break;
				}
			}
		}
		
		switch(type) {
		case "int":
			try {
				lv = new LongValue(next[pos].toLong());

			} catch (InvalidLeaf e) {
				System.err.println("Invalid column type for given function");
				e.printStackTrace();
				System.exit(1);
			}
			break;
		case "decimal":
			try {
				lv = new DoubleValue(next[pos].toDouble());
			} catch (InvalidLeaf e) {
				System.err.println("Invalid column type for given function");
				e.printStackTrace();
				System.exit(1);
			}
			break;
		case "char":
		case "varchar":
		case "string":
			lv = new StringValue(next[pos].toString());
			break;
		case "date":
			lv = new DateValue(" "+next[pos].toString()+" ");
			break;
		default:
			throw new SQLException();
		}
		
		return lv;
	}

}
