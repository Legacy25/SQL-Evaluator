package edu.buffalo.cse562.operators;

import java.sql.SQLException;
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
	private Operator child;
	private LeafValue next[];
	
	private HashMap<Column, ColumnInfo> TypeCache;
	
	private List<SelectItem> selectItems;
	
	public ProjectionOperator(List<SelectItem> selectItems, Operator child) {
		
		this.selectItems = selectItems;
		this.child = child;
		
		childSchema = child.getSchema();
		schema = new Schema("PROJECT [" + childSchema.getTableName() + "]", "__mem__");
		
		TypeCache = new HashMap<Column, ColumnInfo>();
		
		generateSchemaColumns();

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
		next = child.readOneTuple();
		if(next == null)
			return null;
		
		
		
		LeafValue[] ret = new LeafValue[schema.getColumns().size()];
		
		int k = 0;
		Iterator<SelectItem> i = selectItems.iterator();
		while(i.hasNext()) {
			
			SelectItem si = i.next();
			
			if(si instanceof SelectExpressionItem) {
			
				SelectExpressionItem sei = (SelectExpressionItem) si;
				Expression expr = sei.getExpression();
				
				try {
					ret[k] = eval(expr);
				} catch (SQLException e) {
					System.err.println("SQLException");
					e.printStackTrace();
					System.exit(1);
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
				System.err.println("Unrecognized SelectItem");
				System.exit(1);
			}
			
			k++;
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
