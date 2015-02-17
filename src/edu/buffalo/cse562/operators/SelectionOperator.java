package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.Expression;

public class SelectionOperator implements Operator {

	Expression where;
	Operator child;
	
	public SelectionOperator(Expression where, Operator child) {
		this.where = where;
		this.child = child;
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

}
