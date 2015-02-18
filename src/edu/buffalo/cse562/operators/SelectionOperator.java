package edu.buffalo.cse562.operators;

import java.sql.SQLException;

import edu.buffalo.cse562.Evaluate;
import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;

public class SelectionOperator implements Operator {

	Schema schema;
	Expression where;
	Operator child;
	
	public SelectionOperator(Expression where, Operator child) {
		this.where = where;
		this.child = child;
		schema = child.getSchema();
		schema.setTableName("SELECT [" + schema.getTableName() + "]");
	}

	@Override
	public Long[] readOneTuple() {
		Long[] next = child.readOneTuple();
		if(next == null)
			return null;
		
		
		
		//TODO Handle where
		return next;
	}

	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

}
