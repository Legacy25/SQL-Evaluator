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
	private Boolean isAsc;


	public OrderByOperator(Expression expr, Boolean isAsc, Operator child) {
		this.child = child;
		
		TypeCache = new HashMap<Column, ColumnInfo>();
		tempList = new ArrayList<LeafValue[]>();
		
		schema = child.getSchema();
		schema.setTableName("ORDER BY [" + schema.getTableName() + "]");
		
		index = 0;
		this.isAsc = isAsc;
		
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
		
		tempList = sortingRoutine(tempList);
		
		child.reset();
	
	}
	
	
	
	private ArrayList<LeafValue[]> sortingRoutine(ArrayList<LeafValue[]> tempList) {
		
		
		for(int i=1; i<tempList.size(); i++) {
			int j = i;
			boolean condition = false;
			
			do {
				String type = null;
				LeafValue element = tempList.get(0)[column];
				
				if(element instanceof LongValue) {
					type = "int";
				}
				else if(element instanceof DoubleValue) {
					type = "decimal";
				}
				else if(element instanceof StringValue) {
					type = "string";
				}
				else if(element instanceof DateValue) {
					type = "date";
				}
				
				switch(type) {
				case "int":
				case "decimal":
					try {
						if(isAsc) {
							condition = tempList.get(j-1)[column].toDouble() > tempList.get(j)[column].toDouble();
						}
						else {
							condition = tempList.get(j-1)[column].toDouble() < tempList.get(j)[column].toDouble();
						}
					} catch (InvalidLeaf e) {
						
					}
					break;
				case "string":
				case "date":
					if(isAsc) {
						condition = tempList.get(j-1)[column].toString().compareToIgnoreCase
										(tempList.get(j)[column].toString()) > 0;
					}
					else {
						condition = tempList.get(j-1)[column].toString().compareToIgnoreCase
										(tempList.get(j)[column].toString()) < 0;
					}
					break;
					
				
				}
				
			
				if(condition) {
					LeafValue[] temp = tempList.get(j);
					tempList.set(j, tempList.get(j-1));
					tempList.set(j-1, temp);
					
				}
				
				j--;
			} while(j > 0 && condition);
		}
		
		return tempList;
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
