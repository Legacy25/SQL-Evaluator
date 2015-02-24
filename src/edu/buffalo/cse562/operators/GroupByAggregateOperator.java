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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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
	
	private int index, maxpos, currentpos;
	private ArrayList<Double[]> sum, min, max, avg;
	private ArrayList<Integer[]> count;
	
	
	private HashMap<String, Integer> seenValues;
	private ArrayList<Column> columns;
	private boolean[] selectedCols;
	
	private ArrayList<LeafValue[]> output;
	
	public GroupByAggregateOperator(ArrayList<Column> columns, List<SelectItem> selectItems, Operator child) {
		
		this.selectItems = selectItems;
		this.child = child;
		this.columns = columns;
		
		childSchema = child.getSchema();
		schema = new Schema("GROUP BY [" + childSchema.getTableName() + "]", "__mem__");
		
		TypeCache = new HashMap<Column, ColumnInfo>();
		output = new ArrayList<LeafValue[]>();
		
		sum = new ArrayList<Double[]>();
		max = new ArrayList<Double[]>();
		min = new ArrayList<Double[]>();
		avg = new ArrayList<Double[]>();
		count = new ArrayList<Integer[]>();
		
		index = 0;
		
		
		maxpos = -1;
		currentpos = 0;

		selectedCols = new boolean[childSchema.getColumns().size()];
		Arrays.fill(selectedCols, false);
		getSelectedColumns();
		
		seenValues = new HashMap<String, Integer>();
		
		generateSchemaColumns();
		generateOutput();
		
		reset();

	}
	
	
	
	private void addCounterRow() {
		
		int size = selectItems.size();
		
		sum.add(new Double[size]);
		avg.add(new Double[size]);
		max.add(new Double[size]);
		min.add(new Double[size]);
		count.add(new Integer[size]);
		
		
	}
	
	private void resetCounters(int i) {

		Arrays.fill(count.get(i), 0);
		Arrays.fill(avg.get(i), new Double(0));
		Arrays.fill(max.get(i), new Double(0));
		Arrays.fill(min.get(i), new Double(0));
		Arrays.fill(sum.get(i), new Double(0));
	}
	
	
	private void initializeMinMax(int i) {
		
		int k = 0;
		Iterator<SelectItem> i1 = selectItems.iterator();
		while(i1.hasNext()) {
			
			SelectItem si = i1.next();
			
			if(si instanceof SelectExpressionItem) {
			
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Expression expr = sei.getExpression();
				
				if(expr instanceof Function) {
					
					if(((Function) expr).getName().equalsIgnoreCase("MAX")
							|| ((Function) expr).getName().equalsIgnoreCase("MIN")) {
						try {
							min.get(i)[k] = max.get(i)[k] = eval(expr).toDouble();
						} catch (InvalidLeaf | SQLException e) {
							
						}
					}
				}
			}
			k++;
		}
		
	}
	
	
	
	
	private void generateOutput() {
		
		while((next = child.readOneTuple()) != null) {
			
			String key = "";
			
			for(int i=0; i<selectedCols.length; i++) {
				if(selectedCols[i]) {
					key += next[i].toString();
				}
			}
			
			
			if(seenValues.containsKey(key)) {
				currentpos = seenValues.get(key);
			}
			else {
				maxpos++;
				currentpos = maxpos;
				addCounterRow();
				initializeMinMax(currentpos);
				
				output.add(null);
				seenValues.put(key, currentpos);
				
				resetCounters(currentpos);
				
				
			}
			
			LeafValue[] ret = generateReturn();
			output.set(currentpos, ret);
		}
		
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
		
		LeafValue[] ret = new LeafValue[childSchema.getColumns().size() + selectItems.size()];
		
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
				}
				else {

					try {
						ret[k] = eval(expr);
					} catch (SQLException e) {
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
				AllTableColumns atc = (AllTableColumns) si;
				Table t = atc.getTable();
				
				for(int j=0; j<childSchema.getColumns().size(); j++) {
					if(childSchema.getColumns().get(j).getTable().getName().equalsIgnoreCase(t.getName())) {
						schema.addColumn(childSchema.getColumns().get(j));
					}
				}
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
		
		if(index < output.size()) {
			LeafValue[] ret = output.get(index);
			index++;
			return ret;
		}

		
		return null;
		
	}
	
	
	
	
	
	
	
	
	
	public LeafValue[] generateReturn() {
		LeafValue ret[] = new LeafValue[schema.getColumns().size()];
		
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
					ExpressionList paramList = ((Function) expr).getParameters();
					Expression ex = null;
					if(paramList != null) {
						ex = (Expression) ((Function) expr).getParameters().getExpressions().get(0);
					}
					else {
						ex = childSchema.getColumns().get(0);
					}
					
					
					//===============================SUM=======================================
					if(funName.equalsIgnoreCase("SUM")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								sum.get(currentpos)[k] += res;
							}
							
						} catch (SQLException e) {
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.exit(1);
						}
						ret[k] = new DoubleValue(sum.get(currentpos)[k]);

					}
					
					
					
					//===============================AVG=======================================
					else if(funName.equalsIgnoreCase("AVG")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								sum.get(currentpos)[k] += res;
								count.get(currentpos)[k]++;
								avg.get(currentpos)[k] = sum.get(currentpos)[k] / count.get(currentpos)[k];
							}
							
						} catch (SQLException e) {
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.exit(1);
						}
						ret[k] = new DoubleValue(avg.get(currentpos)[k]);		
						
					}
					
					
					
					//===============================COUNT=======================================
					else if(funName.equalsIgnoreCase("COUNT")) {
						
						try {
							if(eval(ex) != null) {
								count.get(currentpos)[k]++;
							}
							
						} catch (SQLException e) {
							System.exit(1);
						}
						ret[k] = new DoubleValue(count.get(currentpos)[k]);		
						
					}
					
					
					
					//===============================MIN=======================================
					else if(funName.equalsIgnoreCase("MIN")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								if(min.get(currentpos)[k] > res)
									min.get(currentpos)[k] = res;
							}
							
						} catch (SQLException e) {
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.exit(1);
						}
						ret[k] = new DoubleValue(min.get(currentpos)[k]);		
						
					}
					
					
					
					
					//===============================MAX=======================================
					else if(funName.equalsIgnoreCase("MAX")) {
						
						try {
							if(eval(ex) != null) {
								double res = eval(ex).toDouble();
								if(max.get(currentpos)[k] < res)
									max.get(currentpos)[k] = res;
							}
							
						} catch (SQLException e) {
							System.exit(1);
						} catch (InvalidLeaf e) {
							System.exit(1);
						}
						ret[k] = new DoubleValue(max.get(currentpos)[k]);		
						
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
						System.exit(1);
					}
				}
				
			}
			
			else if(si instanceof AllTableColumns) {
				AllTableColumns atc = (AllTableColumns) si;
				Table t = atc.getTable();
				
				for(int j=0; j<childSchema.getColumns().size(); j++) {
					if(childSchema.getColumns().get(j).getTable().getName().equalsIgnoreCase(t.getName())) {
						ret[k] = next[j];
						k++;
					}
				}
				
				k--;
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
		index = 0;
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
				System.exit(1);
			}
			break;
		case "decimal":
			try {
				lv = new DoubleValue(next[pos].toDouble());
			} catch (InvalidLeaf e) {
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
