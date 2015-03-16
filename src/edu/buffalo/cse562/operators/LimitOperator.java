package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class LimitOperator implements Operator {

	/*
	 * Limit Operator
	 * 		Scans over the child operator and
	 * 		emits only the first l tuples
	 * 
	 * Constructor Variables
	 * 		l, the number of tuples to emit
	 * 		The child operator
	 * 
	 * Working Set Size - 1
	 */
	
	private Schema schema;			/* Schema for this table */
	

	private long limit;				/* Limit of tuples to emit */
	private Operator child;			/* The child operator */
	
	private long count;				/* Keeps track of the number of tuples
											emitted till now */
	
	
	public LimitOperator(long limit, Operator child) {
		this.limit = limit;
		this.child = child;
		
		/* Schema is unchanged from the child's schema */
		schema = new Schema(child.getSchema());
		
		/* Set an appropriate table name, for book-keeping */
		schema.setTableName("L(" + schema.getTableName() + ")");
		
		/* Initialize the count to 0 */
		count = 0;
	}


	@Override
	public void initialize() {
		child.initialize();
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public LeafValue[] readOneTuple() {
		LeafValue[] next = child.readOneTuple();
		if(next == null) {
			/*
			 * No more tuples
			 */
			return null;
		}
		
		if(count >= limit) {
			/* Limit reached */
			return null;
		}
		else {
			/* 
			 * Increment the counter before emitting the tuple
			 * so that we can keep track of the number of tuples
			 * emitted till now
			 */
			count++;
			return next;
		}
	}

	@Override
	public void reset() {
		/* Reset the count to 0 */
		count = 0;
		
		/* Now reset the child so that we can begin again */
		child.reset();	
	}

}
