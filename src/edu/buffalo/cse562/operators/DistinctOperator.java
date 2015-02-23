package edu.buffalo.cse562.operators;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.schema.Schema;

public class DistinctOperator implements Operator {

	private Schema schema;
	private Operator child;
	private ArrayList<SelectItem> selectItems;
	
	public DistinctOperator(ArrayList<SelectItem> selectItems, Operator child) {
		this.child = child;
		schema = child.getSchema();
		
		this.selectItems = selectItems;
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public LeafValue[] readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		child.reset();
	}

}
