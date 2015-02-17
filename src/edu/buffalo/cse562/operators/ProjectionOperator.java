package edu.buffalo.cse562.operators;

import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ProjectionOperator implements Operator {

	List<ColumnDefinition> selectItems;
	Operator child;
	
	public ProjectionOperator(List<ColumnDefinition> selectItems, Operator child) {
		this.selectItems = selectItems;
		this.child = child;
	}

	@Override
	public Long[] readOneTuple() {
		Long next[] = child.readOneTuple();
		if(next == null)
			return null;
		
		//TODO Handle Projection
		return next;
	}

	@Override
	public void reset() {
		child.reset();
	}

}
