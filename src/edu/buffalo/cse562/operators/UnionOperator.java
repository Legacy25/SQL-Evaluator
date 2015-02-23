package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class UnionOperator implements Operator {

	private Schema schema;
	private Operator child1, child2;
	private boolean child1Done;
	
	public UnionOperator(Operator child1, Operator child2) {
		schema = child1.getSchema();
		schema.setTableName("UNION [" + child1.getSchema().getTableName() + " AND " + child2.getSchema().getTableName() + "]");
		
		this.child1 = child1;
		this.child2 = child2;
		
		child1Done = false;
		
	}
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public LeafValue[] readOneTuple() {
		
		if(!child1Done) {
			LeafValue[] next = child1.readOneTuple();
			if(next == null) {
				child1Done = true;
				return readOneTuple();
			}
			
			return next;
		}
			
		return child2.readOneTuple();
			
	}

	@Override
	public void reset() {
		child1.reset();
		child2.reset();
	}

}
