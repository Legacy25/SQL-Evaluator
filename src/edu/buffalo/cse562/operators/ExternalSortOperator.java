package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class ExternalSortOperator implements Operator {

	private Schema schema;
	
	public ExternalSortOperator() {

	}
	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void generateSchemaName() {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	@Override
	public LeafValue[] readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator getLeft() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator getRight() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLeft(Operator o) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRight(Operator o) {
		// TODO Auto-generated method stub

	}

}
