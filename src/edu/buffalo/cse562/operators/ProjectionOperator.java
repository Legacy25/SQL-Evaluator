package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Eval;
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

public class ProjectionOperator extends Eval implements Operator {

	private Schema schema, childSchema;
	private List<SelectItem> selectItems;
	private Operator child;
	private LeafValue next[];
	private HashMap<Column, ColumnInfo> TypeCache;
	
	private Boolean aggregate;
	private double[] sum, min, max, avg;
	private int[] count;
	
	private class ColumnInfo {
		String type;
		int pos;
		
		public ColumnInfo(String type, int pos) {
			this.type = type;
			this.pos = pos;
		}
	}
	
	public ProjectionOperator(List<SelectItem> selectItems, Operator child) {
		
		this.selectItems = selectItems;
		this.child = child;
		
		childSchema = child.getSchema();
		schema = new Schema("PROJECT [" + childSchema.getTableName() + "]", "__mem__");
		
		TypeCache = new HashMap<Column, ProjectionOperator.ColumnInfo>();
		
		aggregate = false;
		sum = new double[selectItems.size()];
		Arrays.fill(sum, 0);
		min = new double[selectItems.size()];
		Arrays.fill(min, 0);
		max = new double[selectItems.size()];
		Arrays.fill(max, 0);
		avg = new double[selectItems.size()];
		Arrays.fill(avg, 0);
		count = new int[selectItems.size()];
		Arrays.fill(count, 0);
		
		generateSchemaColumns();

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
					aggregate = true;
					
					col.setColumnType("decimal");
					if(((Function) expr).getName().equalsIgnoreCase("COUNT")) {
						col.setColumnType("int");
					}
					try {
						Expression e = (Expression) ((Function) expr).getParameters().getExpressions().get(0);
						double res =  eval(e).toDouble();
						min[k] = max[k] = res;
					} catch (SQLException e) {
						System.err.println("SQLException");
						e.printStackTrace();
						System.exit(1);
					} catch (InvalidLeaf e) {
						System.err.println("Invalid column type for given function");
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
		
		
		
		LeafValue[] ret = new LeafValue[selectItems.size()];
		
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
		
		if(aggregate) {
			LeafValue[] returned = readOneTuple();
			if(returned == null)
				return ret;
			else return returned;
		}

		return ret;			
	}

	@Override
	public void reset() {
		child.reset();
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
				System.exit(1);
			}
			break;
		case "decimal":
			try {
				lv = new DoubleValue(next[pos].toDouble());
			} catch (InvalidLeaf e) {
				System.err.println("Invalid column type for given function");
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
