package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.HashMap;

import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class SelectionOperator extends Eval implements Operator {

	private Schema schema;
	private Expression where;
	private Operator child;
	private LeafValue next[];
	private HashMap<Column, ColumnInfo> TypeCache;
	
	private class ColumnInfo {
		String type;
		int pos;
		
		public ColumnInfo(String type, int pos) {
			this.type = type;
			this.pos = pos;
		}
	}
	
	public SelectionOperator(Expression where, Operator child) {
		this.where = where;
		this.child = child;
		
		TypeCache = new HashMap<Column, SelectionOperator.ColumnInfo>();
		schema = child.getSchema();
		schema.setTableName("SELECT [" + schema.getTableName() + "]");
	}

	@Override
	public LeafValue[] readOneTuple() {
		next = child.readOneTuple();
		if(next == null)
			return null;
		
		try {
			BooleanValue test = (BooleanValue) eval(where);
			if(!test.getValue()) {
				return readOneTuple();
			}
		} catch (SQLException e) {
			System.err.println("SQL Exception");
		}

		return next;
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
			for(int i=0; i<schema.getColumns().size(); i++) {
				if(arg0.getColumnName().equals(schema.getColumns().get(i).getColumnName().toString())
						|| arg0.getWholeColumnName().equals(schema.getColumns().get(i).getColumnName().toString())) {
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "decimal":
			try {
				lv = new DoubleValue(next[pos].toDouble());
			} catch (InvalidLeaf e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "char":
		case "varchar":
		case "string":
			lv = new StringValue(next[pos].toString());
			break;
		case "date":
			lv = new DateValue(next[pos].toString());
			break;
		default:
			throw new SQLException();
		}
		
		return lv;
	}

}
