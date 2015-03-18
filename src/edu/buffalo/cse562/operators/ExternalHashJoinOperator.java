package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.ParseTreeOptimizer;
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
	
	private int joinedLength;
	private ArrayList<LeafValue[]> tempList;		/* Temporary list that holds the joined tuples */
	
	/* Hashes for the children's keys */
	private HashMap<String, ArrayList<LeafValue []>> hash;
	
	/* Boolean array that contains selected columns for both relations */
	private boolean[] selectedCols1;
	private boolean[] selectedCols2;
	
	
	public ExternalHashJoinOperator(Expression where, Operator child1, Operator child2) {
		this.where =  where;
		this.child1 = child1;
		this.child2 = child2;
		
		/* Initializations */
		selectedCols1 = new boolean[child1.getSchema().getColumns().size()];
		selectedCols2 = new boolean[child2.getSchema().getColumns().size()];
		joinedLength = selectedCols1.length + selectedCols2.length;
		
		Arrays.fill(selectedCols1, false);
		Arrays.fill(selectedCols2, false);
		
		tempList = new ArrayList<LeafValue[]>();
		hash = new HashMap<String, ArrayList<LeafValue[]>>(10000, (float) 0.5);
		
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
		
		ArrayList<Expression> clauseList = new ArrayList<Expression>();
		
		if(where instanceof AndExpression) {
			clauseList.addAll(ParseTreeOptimizer.splitAndClauses(where));
		}
		else {
			clauseList.add(where);
		}
		
		for(Expression clause : clauseList) {
			BinaryExpression binaryClause = (BinaryExpression) clause;
			Column left = (Column) binaryClause.getLeftExpression();
			Column right = (Column) binaryClause.getRightExpression();
			
			int pos = findInSchema(left, child1.getSchema());
			if(pos < 0) {
				selectedCols2[findInSchema(left, child2.getSchema())] = true;
				selectedCols1[findInSchema(right, child1.getSchema())] = true;
			}
			else {
				selectedCols1[pos] = true;
				selectedCols2[findInSchema(right, child2.getSchema())] = true;
			}
			
		}
	}
	
	private int findInSchema(Column col, Schema schema) {

		ArrayList<ColumnWithType> columnList = schema.getColumns();
		for(int i=0; i<columnList.size(); i++) {
			if(columnList.get(i).getWholeColumnName().equalsIgnoreCase(col.getWholeColumnName()))
				return i;
		}
		
		return -1;
	}

	public void buildHash(){
		
		LeafValue[] next;

		child1.initialize();
		
		while((next = child1.readOneTuple()) != null) {
			String key = "";
			for(int i=0; i<selectedCols1.length; i++) {
				if(selectedCols1[i])
					key += next[i].toString();
			}
			
			if(!hash.containsKey(key)) {
				ArrayList<LeafValue[]> toBeAdded = new ArrayList<LeafValue[]>();
				toBeAdded.add(next);
				hash.put(key, toBeAdded);
				continue;
			}

			hash.get(key).add(next);
		}

		child1.reset();
		
	}
	
	@Override
	public LeafValue[] readOneTuple() {
		if(tempList.isEmpty()) {
			/* 
			 * no more tuples to return
			 */
			return null; 
		}
		
		/* Temporary return tuple */
		LeafValue[] next = tempList.get(0);
		
		/* Remove tuple from tempList */
		tempList.remove(0);
		
		return next;
	}
	
	public void buildJoin(){
		
		LeafValue[] next = null;
		
		while((next = child2.readOneTuple()) != null) {
			String key = "";
			for(int i=0; i<selectedCols2.length; i++) {
				if(selectedCols2[i])
					key += next[i].toString();
			}
			
			ArrayList<LeafValue[]> matchedTuples = hash.get(key);
			
			if(matchedTuples == null) {
				continue;
			}
			
			for(LeafValue[] left : matchedTuples) {
				LeafValue[] joinedTuple = new LeafValue[joinedLength];
				for(int i=0; i<joinedLength; i++) {
					if(i < selectedCols1.length) {
						joinedTuple[i] = left[i];
					}
					else {
						joinedTuple[i] = next[i - selectedCols1.length];
					}
				}
				tempList.add(joinedTuple);
			}
			
		}

		/* Clear the caches, we don't need them anymore */
		hash.clear();
	}
	
	
	public void reset(){
		/* First reset the children */
		child1.reset();
		child2.reset();
		
		tempList.clear();
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
