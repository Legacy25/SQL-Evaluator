package edu.buffalo.cse562.operators;


import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class DistinctOperator implements Operator {
	
	/*
	 * Distinct Operator
	 * 		Scans over the child operator and
	 * 		emits only one copy of same tuples
	 * 
	 * Constructor Variables
	 * 		The child operator
	 * 
	 * Working Set Size - The size of the seenValues HashMap
	 * 
	 * To make the working set size 1, we can first sort, then iterate
	 * over the sorted list, emitting only the first occurrence of a
	 * tuple. We would need to keep only one key in memory, instead of
	 * the list of all seen values till now.
	 */
	
	private Schema schema;			/* Schema for this table */
	

	private Operator child;			/* The child operator */
	
	private HashMap<String, Boolean> seenValues;		/* Keeps a record
															of seen values till now.
															The Boolean is a bogus
	 														value, we just need to
	 														check for the presence
	 														of the key in the map. */
	
	
	public DistinctOperator(Operator child) {
		this.child = child;
		
		/* Schema is unchanged from the child's schema */
		schema = new Schema(child.getSchema());
		
		/* Set an appropriate table name, for book-keeping */
		schema.setTableName("\u03B4(" + schema.getTableName() + ")");
		
		/* Initializations */
		seenValues = new HashMap<String, Boolean>();
	}
	
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void initialize() {
		child.initialize();
	}

	@Override
	public LeafValue[] readOneTuple() {
		/* Necessary initialization */
		LeafValue[] next = null;
		
		/* Keep getting tuples till we find one we haven't seen yet or we run out */
		while ((next = child.readOneTuple()) != null) {
			
			/* Generate a key for the tuple, by concatenating each attribute value */
			String key = "";
			for(int i=0; i<schema.getColumns().size(); i++) {
				key += next[i].toString();
			}
			
			/* Convert to lower-case to make it case insensitive */
			key = key.toLowerCase();
			
			/* If we have seen this value, disregard this tuple */
			if(seenValues.containsKey(key))
				continue;
			
			/* Otherwise, add this to the map of seen value and return it */
			seenValues.put(key, true);
			return next;
		}
		
		/* No more tuples, so return null */
		return null;
	}

	@Override
	public void reset() {
		/* First empty the list of seen values */
		seenValues.clear();
		
		/* Then reset the child */
		child.reset();
	}

	@Override
	public Operator getLeft() {
		return child;
	}

	@Override
	public Operator getRight() {
		return null;
	}

}
