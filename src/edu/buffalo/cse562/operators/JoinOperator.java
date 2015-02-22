package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class JoinOperator implements Operator {

	private Schema schema;
	private Operator child1, child2;
	private LeafValue next1[];
	private LeafValue next2[];
	boolean flag;

	public JoinOperator(Operator child1, Operator child2) {
		
		this.child1 = child1;
		this.child2 = child2;
		generateSchema();
		
		next1 = null;
		next2 = null;
		flag = true;
	}
	
	private void generateSchema() {

		schema = new Schema(child1.getSchema().getTableName()+" JOIN "+child2.getSchema().getTableName(), "__mem__");

		for(ColumnWithType c:child1.getSchema().getColumns()) {
			schema.addColumn(c);
		}
		
		for(ColumnWithType c:child2.getSchema().getColumns()) {
			schema.addColumn(c);
		}
		
	}
	
	@Override
	public LeafValue[] readOneTuple() {
		
		
		if(flag) {
			flag = false;
			next1 = child1.readOneTuple();
			if(next1 == null)
				return null;
		}

		next2 = child2.readOneTuple();
		if(next2 == null) {
			flag = true;
			child2.reset();
			return readOneTuple();
		}
		
		if(next1 == null)
			return null;
		
		int length = next1.length + next2.length;
		
		LeafValue ret[] = new LeafValue[length];
		for(int i=0; i<length; i++) {
			if(i<next1.length)
				ret[i] = next1[i];
			else
				ret[i] = next2[i-next1.length];
		}
		
		return ret;
	}

	@Override
	public void reset() {
		child1.reset();
		child2.reset();
		flag = true;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

}
