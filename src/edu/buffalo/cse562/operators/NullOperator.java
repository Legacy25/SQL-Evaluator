package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class NullOperator implements Operator {

	private Schema schema = new Schema();
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void generateSchemaName() {

	}

	@Override
	public void initialize() {

	}

	@Override
	public LeafValue[] readOneTuple() {
		return null;
	}

	@Override
	public void reset() {

	}

	@Override
	public Operator getLeft() {
		return null;
	}

	@Override
	public Operator getRight() {
		return null;
	}

	@Override
	public void setLeft(Operator o) {

	}

	@Override
	public void setRight(Operator o) {

	}

}
