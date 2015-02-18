package edu.buffalo.cse562.operators;

import edu.buffalo.cse562.schema.Schema;

public interface Operator {

	public Schema getSchema();
	
	public Long[] readOneTuple();
	public void reset();
	
}
