package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import edu.buffalo.cse562.ParseTreeOptimizer;
import edu.buffalo.cse562.schema.ColumnInfo;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class ExternalHashJoinOperator implements Operator {

	/*
	 * External Hash Join Operator
	 * 		Build hashes over both child relations,
	 * 		and join them in memory
	 * 
	 * Constructor Variables
	 * 		A where predicate to give the join clause
	 * 		Two child operators
	 * 
	 * Working Set Size - Size of both relations plus their hash key sets
	 */	
	
	private Schema schema;			/* Schema for this table */
	
	
	private Expression where;				/* The join clause */
	private Operator child1, child2;		/* The two relations to cross product */
	
	
	private int index;						/* Index into tempList */
	private ArrayList<LeafValue[]> tempList;		/* Temporary list that holds the joined tuples */
	
	/* Hashes for the children's keys */
	private HashMap<String, ArrayList<LeafValue []>> JoinCache1;
	private HashMap<String, ArrayList<LeafValue []>> JoinCache2;
	
	/* Boolean array that contains selected columns for both relations */
	private boolean[] selectedCols1;
	private boolean[] selectedCols2;
	
	
	public ExternalHashJoinOperator(Expression where, Operator child1, Operator child2) {
		this.where =  where;
		this.child1 = child1;
		this.child2 = child2;
		
		/* Initializations */
		index = 0;
		
		tempList = new ArrayList<LeafValue[]>();
		JoinCache1 = new HashMap<String,ArrayList<LeafValue[]>>();
		JoinCache2 = new HashMap<String,ArrayList<LeafValue[]>>();
		
		buildSchema();
	}
		
	private void buildSchema() {
		
		schema = new Schema();
		generateSchemaName();
		
		for(ColumnWithType c:child1.getSchema().getColumns()) {
			schema.addColumn(c);
		}
		
		for(ColumnWithType c:child2.getSchema().getColumns()) {
			schema.addColumn(c);
		}
		
	}
	
	@Override
	public void generateSchemaName() {
		child1.generateSchemaName();
		child2.generateSchemaName();
		
		schema.setTableName(
				child1.getSchema().getTableName() +
				" \u2A1D " +
				child2.getSchema().getTableName()
				);
		
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}
	
	@Override
	public void initialize() {
		/* First initialize the children */
		child1.initialize();
		child2.initialize();
		
		/* Get the columns on which to build the key */
		getSelectedColumns();
		
		/* Build the hash on both tables */
		buildHash();
		
		/* Generate the temporary list of joined tuples */
		buildJoin();
	}
	
	public void getSelectedColumns() {
		Expression splitAndClauses(Expression e);
		{
		  ArrayList<Expression> ret = 
		     new ArrayList<Expression>();
		  if(e instanceof AndExpression){
		    AndExpression a = (AndExpression)e;
		    ret.addAll(
		    		ParseTreeOptimizer.splitAndClauses(a.getLeftExpression())
		    );
		    ret.addAll(
		    		ParseTreeOptimizer.splitAndClauses(a.getRightExpression())
		    );
		  } else {
		    ret.add(e);
		  }
		}
	}
	
	public void buildHash(){
		
		String key1 = "";
		String key2 = "";
		
		child1.initialize();
		child2.initialize();
		
		LeafValue[] next1, next2;
		
		while((next1 = child1.readOneTuple()) != null) {
			while((next2 = child2.readOneTuple()) != null) {
				for(int i=0; i<selectedCols1.length; i++) {
					key1 += next1[i].toString();
				}
				for(int i=0; i<selectedCols2.length; i++) {
					key2 += next2[i].toString();
				}
				
				if(JoinCache1.containsKey(key1)) {
					JoinCache1.get(key1).add(next1);
				}
				else {
					ArrayList<LeafValue[]> toBeAdded = new ArrayList<LeafValue[]>();
					toBeAdded.add(next1);
					JoinCache1.put(key1, toBeAdded);
				}
				
				if(JoinCache2.containsKey(key2)) {
					JoinCache1.get(key2).add(next2);
				}
				else {
					ArrayList<LeafValue[]> toBeAdded = new ArrayList<LeafValue[]>();
					toBeAdded.add(next2);
					JoinCache1.put(key2, toBeAdded);
				}
			}
			
			child2.reset();
		}
		
		child1.reset();
		
	}
	
	@Override
	public LeafValue[] readOneTuple() {
		if(index >= tempList.size()) {
			/* If index reaches size limit, return null,
			 * no more tuples to return
			 */
			return null; 
		}
		
		/* Temporary return tuple */
		LeafValue[] next = tempList.get(index);
		
		/* Increment the index */
		index++;
		
		return next;
	}
	
	public void buildJoin(){
		
		Set<String> key = JoinCache1.entrySet();
		Iterator<String> i = key.iterator();
		while(i.hasNext()){
			if(JoinCache1.containKey(Key)){
				
			}
		}
	}
	
	
	public void reset(){
		/* First reset the children */
		child1.reset();
		child2.reset();
		
		/* Reset the index */
		index = 0; 
		
		/* Clear the caches */
		JoinCache1.clear();
		JoinCache2.clear();
		
	}
	
	@Override
	public Operator getLeft() {
		return child1;
	}
	
	@Override
	public Operator getRight() {
		return child2;
	}
	
	@Override
	public void setLeft(Operator o) {
		child1 = o;
	}
	
	@Override
	public void setRight(Operator o) {
		child2 = o;
	}
}
