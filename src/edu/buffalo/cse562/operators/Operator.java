package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public interface Operator {

	public Schema getSchema();

	public void initialize();
	public LeafValue[] readOneTuple();
	public void reset();
	
	public Operator getLeft();
	public Operator getRight();
	
}
