package edu.buffalo.cse562.operators;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.Schema;

public class MemScanOperator implements Operator {

private Schema schema;
	
	
	int index = 0;

	public MemScanOperator(Schema schema) {		
		/* 
		 * Get the initial schema,
		 * the schemas for the rest of the operators
		 * are ultimately generated
		 * using this information
		 */
		this.schema = schema;
	}
	
	public void generateSchemaName() {
		
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void initialize() {
		index = 0;
	}
	
	@Override
	public LeafValue[] readOneTuple() {
		LeafValue[] ret = null;
		try {
			ret = schema.getTuple(index);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		index++;
		return ret;
	}

	@Override
	public void reset() {
		initialize();
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
