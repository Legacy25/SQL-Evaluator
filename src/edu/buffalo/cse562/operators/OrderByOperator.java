package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

public class OrderByOperator implements Operator {

	/*
	 * Order By Order
	 * 		Sorts the child relation
	 * 
	 * Constructor Variables
	 * 		An expression - The sort key attribute
	 * 		A boolean which determines the sort order, ascending or descending
	 * 		The child operator
	 * 
	 * Working Set Size - The size of the relation
	 */
	
	private Schema schema;			/* Schema for this table */
	
	
	private Expression expr;		/* Kept if needed in future */
	private Boolean isAsc;			/* Ascending order if true, descending otherwise */
	private Operator child;			/* The child operator to be sorted */
	
	/* Holds the sorted relation in memory */
	private ArrayList<LeafValue[]> tempList;		
	
	/* Holds the index into tempList of the next tuple to be emitted */
	private int index;
	
	/* Holds the index of the sort key attribute */
	private int column;

	

	public OrderByOperator(Expression expr, Boolean isAsc, Operator child) {
		this.expr = expr;
		this.isAsc = isAsc;
		this.child = child;
		
		/* Schema is unchanged from the child's schema */
		schema = new Schema(child.getSchema());
		
		/* Set an appropriate table name, for book-keeping */
		generateSchemaName();
		
		/* Initializations */
		tempList = new ArrayList<LeafValue[]>();
		index = 0;
		column = 0;
		findColumn(this.expr.toString());

	}

	public void generateSchemaName() {
		child.generateSchemaName();
		schema.setTableName("L(" + child.getSchema().getTableName() + ")");
	}
	
	/*
	 * Helper function to find the appropriate column index on which to sort
	 */
	private void findColumn(String columnName) {
		
		for(int i=0; i<schema.getColumns().size(); i++) {
			if(schema.getColumns().get(i).getColumnName().equalsIgnoreCase(columnName)
					|| schema.getColumns().get(i).getWholeColumnName().equalsIgnoreCase(columnName)) {
				
				column = i;
				break;
				
			}
		}
		
	}
	

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void initialize() {
		child.initialize();
		
		/* 
		 * Since this is an in-memory operation, initialize will load the
		 * entire child relation into memory before sorting and then sort
		 * it in memory
		 */
		sort();
	}

	@Override
	public LeafValue[] readOneTuple() {
		/* Emit the next tuple in tempList, if any */
		if(index < tempList.size()) {
			LeafValue[] ret = tempList.get(index);
			index++;
			return ret;
		}

		/* Reached end of tempList, no more tuples to reurn */
		return null;
	}

	@Override
	public void reset() {
		/* First set the index to 0 */
		index = 0;
		
		/* Then reset the child */
		child.reset();
	}

	
	private void sort() {
		/* The sort function, calls on the sorting routine */
		
		/* Load entire child relation into tempList */
		LeafValue[] next = child.readOneTuple();
		while (next != null) {
			tempList.add(next);
			next = child.readOneTuple();
		}
		
		/* Sort tempList using the sorting routine */
		Collections.sort(tempList, new Comparator<LeafValue[]>() {

			@Override
			public int compare(LeafValue[] o1, LeafValue[] o2) {
				String type = "";
				Boolean condition = false;
				LeafValue element = o1[column];
				
				if(element instanceof LongValue) {
					type = "int";
				}
				else if(element instanceof DoubleValue) {
					type = "decimal";
				}
				else if(element instanceof StringValue) {
					type = "string";
				}
				else if(element instanceof DateValue) {
					type = "date";
				}
				
				switch(type) {
				case "int":
				case "decimal":
					try {
						if(o1[column].toDouble() == o2[column].toDouble())
							return 0;
						if(isAsc) {
							condition = o1[column].toDouble() > o2[column].toDouble();
						}
						else {
							condition = o1[column].toDouble() < o2[column].toDouble();
						}
					} catch (InvalidLeaf e) {
						
					}
					break;
				case "string":
				case "date":
					if(o1[column].toString().equalsIgnoreCase(o2[column].toString()))
						return 0;
					if(isAsc) {
						condition = o1[column].toString().compareToIgnoreCase
										(o2[column].toString()) > 0;
					}
					else {
						condition = o1[column].toString().compareToIgnoreCase
										(o2[column].toString()) < 0;
					}
					break;
					
				
				}
				
				if(condition)
					return 1;
				else
					return -1;
			}
			
		});
		
		/* 
		 * Reset the child since we read it
		 * This preserves the semantics of the Operator interface
		 */
		child.reset();
	
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
		child = o;
	}

	@Override
	public void setRight(Operator o) {

	}

}
