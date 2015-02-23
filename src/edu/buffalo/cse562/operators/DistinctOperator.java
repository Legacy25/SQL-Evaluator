package edu.buffalo.cse562.operators;


import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class DistinctOperator implements Operator {

	private Schema schema;
	private Operator child;
	
	private HashMap<String, Boolean> seenValues;
	
	public DistinctOperator(Operator child) {
		this.child = child;
		schema = child.getSchema();
		
		schema.setTableName("DISTINCT [" + schema.getTableName() + "]");
		seenValues = new HashMap<String, Boolean>();
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
		
		String key = "";
		for(int i=0; i<schema.getColumns().size(); i++) {
			key += next[i].toString();
		}
		
		key = key.toLowerCase();
		if(seenValues.containsKey(key))
			return readOneTuple();
		
		seenValues.put(key, true);
		return next;
	}

	@Override
	public void reset() {
		child.reset();
		seenValues.clear();
	}

}
