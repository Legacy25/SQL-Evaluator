package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class LimitOperator implements Operator {

	private Schema schema;
	private Operator child;
	private long limit;
	private long count;
	
	public LimitOperator(long l, Operator child) {
		this.limit = l;
		this.child = child;
		
		schema = child.getSchema();
		schema.setTableName("LIMIT [" + schema.getTableName() + "]");
		
		count = 0;
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public LeafValue[] readOneTuple() {
		LeafValue[] next = child.readOneTuple();
		if(next == null)
			return null;
		
		if(count >= limit) {
			return null;
		}
		else {
			count++;
			return next;
		}
	}

	@Override
	public void reset() {
		child.reset();	
		count = 0;
	}

}
