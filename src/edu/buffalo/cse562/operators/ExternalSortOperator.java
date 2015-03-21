package edu.buffalo.cse562.operators;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.schema.Schema;

public class ExternalSortOperator implements Operator {
	
	/*
	 * External Sort Operator
	 * 		Sorts the child relation without blocking
	 * 
	 * Constructor Variables
	 * 		Order By attributes list
	 * 		The child operator
	 * 
	 * Working Set Size - Memory buffer size
	 */
	private Schema schema;				/* Schema for this table */
	
	private Operator child;				/* The child operator to be sorted */
	
	
	/* Holds the sort key attributes */
	private ArrayList<OrderByElement> arguments;
	
	
	
	public ExternalSortOperator(ArrayList<OrderByElement> arguments, Operator child) {
		this.arguments = arguments;
		this.child = child;
	}
	
	public ExternalSortOperator(OrderByOperator o) {
		this.arguments = o.getArguments();
		this.child = o.getLeft();
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void generateSchemaName() {
		child.generateSchemaName();
		schema.setTableName("O(" + child.getSchema().getTableName() + ")");
	}

	@Override
	public void initialize() {
		child.initialize();
		
		/* TODO Other initializations go here */
	}

	@Override
	public LeafValue[] readOneTuple() {
		
		/* TODO Write this */
		return null;
		
	}

	@Override
	public void reset() {
		child.reset();
		
		/* TODO Other state clean ups go here */
	}

	@Override
	public Operator getLeft() {
		return child;
	}

	@Override
	public Operator getRight() {
		return null;
	}

	@Override
	public void setLeft(Operator o) {
		this.child = o;
	}

	@Override
	public void setRight(Operator o) {
		/* Nothing to do */
	}

}
