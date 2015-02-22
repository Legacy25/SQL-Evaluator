package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.schema.ColumnInfo;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByAggregateOperator extends Eval implements Operator {

	private Schema schema, childSchema;
	private Operator child;
	private LeafValue next[];
	
	private HashMap<Column, ColumnInfo> TypeCache;
	
	private List<SelectItem> selectItems;
	
	private double[] sum, min, max, avg;
	private int[] count;
	
	
	private String currentKey;
	private HashMap<String, Boolean> seenValues;
	private ArrayList<Column> columns;
	private boolean[] selectedCols;
	
	public GroupByAggregateOperator(ArrayList<Column> columns, List<SelectItem> selectItems, Operator child) {
		
		this.selectItems = selectItems;
		this.child = child;
		this.columns = columns;
		
		childSchema = child.getSchema();
		schema = new Schema("GROUP BY [" + childSchema.getTableName() + "]", "__mem__");
		
		TypeCache = new HashMap<Column, ColumnInfo>();
		
		sum = new double[selectItems.size()];
		min = new double[selectItems.size()];
		max = new double[selectItems.size()];
		avg = new double[selectItems.size()];
		count = new int[selectItems.size()];
		
		Arrays.fill(count, 0);
		Arrays.fill(avg, 0);
		Arrays.fill(max, 0);
		Arrays.fill(min, 0);
		Arrays.fill(sum, 0);

		currentKey = "";

		selectedCols = new boolean[childSchema.getColumns().size()];
		Arrays.fill(selectedCols, false);
		getSelectedColumns();
		
		seenValues = new HashMap<String, Boolean>();
		
		generateSchemaColumns();
		reset();

	}
	
	private void getSelectedColumns() {
		
		ArrayList<ColumnWithType> schemaCols = childSchema.getColumns();
		
		for(int i=0; i<columns.size(); i++) {
			for(int j=0; j<schemaCols.size(); j++) {
				if(columns.get(i).getColumnName().equalsIgnoreCase(schemaCols.get(j).getColumnName())
						|| columns.get(i).getWholeColumnName().equalsIgnoreCase(schemaCols.get(j).getWholeColumnName())) {
					selectedCols[j] = true;
				}
			}
		}
		
	}
	
	
	private void generateSchemaColumns() {
		next = child.readOneTuple();
		if(next == null)
			return;		
		
		LeafValue[] ret = new LeafValue[selectItems.size()];
		
		int k = 0;
		Iterator<SelectItem> i = selectItems.iterator();
		while(i.hasNext()) {
			
			SelectItem si = i.next();
			
			if(si instanceof SelectExpressionItem) {
			
				ColumnWithType col = new ColumnWithType(new Table(), null, null, k);
				
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Expression expr = sei.getExpression();
				
				col.setColumnName(expr.toString());
				if(sei.getAlias() != null) {
					col.setColumnName(sei.getAlias());
				}

				if(expr instanceof Function) {
					
					col.setColumnType("decimal");
					if(((Function) expr).getName().equalsIgnoreCase("COUNT")) {
						col.setColumnType("int");
					}
					try {
						Expression e = (Expression) ((Function) expr).getParameters().getExpressions().get(0);
						if(((Function) expr).getName().equalsIgnoreCase("MIN") || ((Function) expr).getName().equalsIgnoreCase("MAX")) {
							double res =  eval(e).toDouble();
							min[k] = max[k] = res;
						}
					} catch (SQLException e) {
						System.err.println("SQLException");
						e.printStackTrace();
						System.exit(1);
					} catch (InvalidLeaf e) {
						System.err.println("Invalid column type for given function");
						e.printStackTrace();
						System.exit(1);
					}
				}
				else {

					try {
						ret[k] = eval(expr);
					} catch (SQLException e) {
						System.err.println("SQLException");
						e.printStackTrace();
						System.exit(1);
					}
					
					
					
					if(ret[k] instanceof LongValue) {
						col.setColumnType("int");
					}
					else if(ret[k] instanceof DoubleValue) {
						col.setColumnType("decimal");
					}
					else if(ret[k] instanceof StringValue) {
						col.setColumnType("string");
					}
					else if(ret[k] instanceof DateValue) {
						col.setColumnType("date");
					}
				}
				schema.addColumn(col);
			}
			
			else if(si instanceof AllTableColumns) {
				// TODO
			}
			else {
				System.err.println("Unrecognized SelectItem)");
				System.exit(1);
			}
			
			k++;
		}
		
		reset();
	}
	
	
	
	@Override
	public LeafValue[] readOneTuple() {
		next = child.readOneTuple();
		if(next == null)
			return null;
		
		
		String key = "";
		
		for(int i=0; i<selectedCols.length; i++) {
			if(selectedCols[i]) {
				key += next[i].toString();
			}
		}
		
		if(currentKey == "") {
			if(seenValues.containsKey(key)) {
				return readOneTuple();
			}
			else {
				currentKey = key;
			}
		}
		
		if(key.equals(currentKey)) {

			LeafValue[] ret = generateReturn();
			LeafValue[] readT = readOneTuple();
			
			if(readT == null) {
				child.reset();
					
				Arrays.fill(count, 0);
				Arrays.fill(avg, 0);
				Arrays.fill(max, 0);
				Arrays.fill(min, 0);
				Arrays.fill(sum, 0);
				
				seenValues.put(currentKey, true);

				currentKey = "";
				return ret;
			}
			else {
				return readT;
			}
		}
		
		return readOneTuple();
		
		
	}
	
	
	
	public LeafValue[] generateReturn() {
		LeafValue ret[] = new LeafValue[selectItems.size()];
		
		int k = 0;
		Iterator<SelectItem> i = selectItems.iterator();
		while(i.hasNext()) {
			
			SelectItem si = i.next();
			
			if(si instanceof SelectExpressionItem) {
			
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Expression expr = sei.getExpression();
				
				if(expr instanceof Function) {
					Function fun = (Function) expr;
					String funName = fun.getName();
					Expression ex = (Expression) ((Function) expr).getParameters().getExpressions().get(0);
					
					
					
					//===============================SUM=======================================
					if(funName.equalsIgnoreCase("SUM")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								sum[k] += res;
							}
							
						} catch (SQLException e) {
							System.err.println("SQLException");
							e.printStackTrace();
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.err.println("Invalid column type for given function");
							e.printStackTrace();
							System.exit(1);
						}
						ret[k] = new DoubleValue(sum[k]);							

					}
					
					
					
					//===============================AVG=======================================
					else if(funName.equalsIgnoreCase("AVG")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								sum[k] += res;
								count[k]++;
								avg[k] = sum[k] / count[k];
							}
							
						} catch (SQLException e) {
							System.err.println("SQLException");
							e.printStackTrace();
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.err.println("Invalid column type for given function");
							e.printStackTrace();
							System.exit(1);
						}
						ret[k] = new DoubleValue(avg[k]);		
						
					}
					
					
					
					//===============================COUNT=======================================
					else if(funName.equalsIgnoreCase("COUNT")) {
						
						try {
							if(eval(ex) != null) {
								count[k]++;
							}
							
						} catch (SQLException e) {
							System.err.println("SQLException");
							e.printStackTrace();
							System.exit(1);
						}
						ret[k] = new DoubleValue(count[k]);		
						
					}
					
					
					
					//===============================MIN=======================================
					else if(funName.equalsIgnoreCase("MIN")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								if(min[k] > res)
									min[k] = res;
							}
							
						} catch (SQLException e) {
							System.err.println("SQLException");
							e.printStackTrace();
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.err.println("Invalid column type for given function");
							e.printStackTrace();
							System.exit(1);
						}
						ret[k] = new DoubleValue(min[k]);		
						
					}
					
					
					
					
					//===============================MAX=======================================
					else if(funName.equalsIgnoreCase("MAX")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								if(max[k] < res)
									max[k] = res;
							}
							
						} catch (SQLException e) {
							System.err.println("SQLException");
							e.printStackTrace();
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.err.println("Invalid column type for given function");
							e.printStackTrace();
							System.exit(1);
						}
						ret[k] = new DoubleValue(max[k]);		
						
					}
					
					
					
					//==========================================================================
					else {
						System.err.println("Unsupported aggregate function "+funName);
					}
					
					
					
				}
				
				
				else {
					try {
						ret[k] = eval(expr);
					} catch (SQLException e) {
						System.err.println("SQLException");
						e.printStackTrace();
						System.exit(1);
					}
				}
				
			}
			
			else if(si instanceof AllTableColumns) {
				// TODO
			}
			else {
				System.err.println("Unrecognized SelectItem)");
				System.exit(1);
			}
			
			k++;
		}
		
		return ret;
	}
	
	
	

	@Override
	public void reset() {
		child.reset();

		seenValues.clear();
	}

	@Override
	public Schema getSchema() {
		return schema;
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

			for(int i=0; i<childSchema.getColumns().size(); i++) {
				
				if(arg0.getWholeColumnName().equalsIgnoreCase(childSchema.getColumns().get(i).getWholeColumnName().toString())
						|| arg0.getWholeColumnName().equalsIgnoreCase(childSchema.getColumns().get(i).getColumnName().toString())) {
					type = childSchema.getColumns().get(i).getColumnType();
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
