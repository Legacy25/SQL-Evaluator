package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.operators.CrossProductOperator;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.SelectionOperator;
import edu.buffalo.cse562.schema.Schema;

public class ParseTreeOptimizer {

	private enum ClauseApplicability {
		ROOT, LEFT, RIGHT;
	}
	
	/* 
	 * The entry-point to the optimizer,
	 * we try to define several functions which
	 * apply a variety of rewriting techniques to optimize
	 * the query plan
	 */
	public static void optimize(Operator parseTree) {
		/*
		 * Query plan rewrites for faster evaluation
		 */
		
		/* Decompose Select Clauses and push them down appropriately */
		parseTree = decomposeAndPushDownSelects(parseTree);
		
		/* Replace Selection over Cross Product with appropriate Join */
		parseTree = findJoinPatternAndReplace(parseTree);
		
		/* Other Patterns go here */
		
		/* Generate appropriate table names after optimization */
		parseTree.generateSchemaName();
		
	}
	
	/* Pushing down Selects through Cross Products - helper methods */
	
	private static ClauseApplicability checkClauseApplicability(
			Schema left, Schema right, Expression clause
			) {
		
		/* Captures a lot of the expressions we can actually reorder */
		if(clause instanceof BinaryExpression) {
			BinaryExpression binaryClause = (BinaryExpression) clause;
			Expression leftClause = binaryClause.getLeftExpression();
			
			if( ! (leftClause instanceof Column) ) {
				/* This clause type not supported, default to ROOT */
				return ClauseApplicability.ROOT;
			}

			/* Checking which schema the left column belongs to */
			ClauseApplicability ret = belongsToSchema(
					left ,
					right ,
					(Column) leftClause
					);
			
			Expression rightClause = binaryClause.getRightExpression();
			
			/* If the rightClause is also a Column */
			if(rightClause instanceof Column) {
				if(ret != belongsToSchema(
						left ,
						right ,
						(Column) rightClause)
						) {
					/* 
					 * If the two do not agree, the only option can be root,
					 * which is there by default, we set ret to null
					 * to avoid returning before we reach the default 
					 */
					ret = null;
				}
			}			

			/* 
			 * ret not null means that even if there are two columns
			 * in the expression, they agree on which side of
			 * the schemas they fall on, so just return one of
			 * the two values, even if the agreed value is ROOT itself
			 */
			if(ret != null)
				return ret;
		}
		
		/* 
		 * Default option, the clause stays where it is, 
		 * either we do not know enough about it to reorder it,
		 * or they do not agree on which side they fall on
		 */
		return ClauseApplicability.ROOT;
	}
	
	private static ClauseApplicability 
		belongsToSchema(Schema left, Schema right, Column column) {
		
		for(Column col : left.getColumns()) {
			if(column.getWholeColumnName().equalsIgnoreCase(col.getWholeColumnName()))
				return ClauseApplicability.LEFT;
		}
		
		for(Column col : right.getColumns()) {
			if(column.getWholeColumnName().equalsIgnoreCase(col.getWholeColumnName()))
				return ClauseApplicability.RIGHT;
		}
		return ClauseApplicability.ROOT;
	}

	
	
	private static Operator decomposeAndPushDownSelects(Operator parseTree) {
		if(parseTree == null) {
			/* Leaf Node */
			return null;
		}
		
		if(parseTree instanceof SelectionOperator) {
			SelectionOperator select = (SelectionOperator) parseTree;
			Operator child = select.getLeft();
			
			if(child instanceof CrossProductOperator) {
				CrossProductOperator crossProduct = (CrossProductOperator) child;
				Operator crossProductLeftChild = crossProduct.getLeft();
				Operator crossProductRightChild = crossProduct.getRight();
				
				ArrayList<Expression> clauseList = splitAndClauses(select.getWhere());
				
				ArrayList<Expression> parentList = new ArrayList<Expression>();
				ArrayList<Expression> leftList = new ArrayList<Expression>();
				ArrayList<Expression> rightList = new ArrayList<Expression>();
				
				for(Expression clause : clauseList) {
					ClauseApplicability ca = checkClauseApplicability(
							crossProductLeftChild.getSchema() , 
							crossProductRightChild.getSchema() ,
							clause
							);
					
					switch(ca) {
					case ROOT:
						parentList.add(clause);
						break;
					case LEFT:
						leftList.add(clause);
						break;
					case RIGHT:
						rightList.add(clause);
						break;
					}
				}
				
				if(!parentList.isEmpty()) {
					Expression where = mergeClauses(parentList);
					parseTree = new SelectionOperator(where, crossProduct);
				}
				else {
					parseTree = crossProduct;
				}
				
				if(!leftList.isEmpty()) {
					Expression where = mergeClauses(leftList);
					crossProduct.setLeft(new SelectionOperator(where, crossProductLeftChild));
				}
				
				if(!rightList.isEmpty()) {
					Expression where = mergeClauses(rightList);
					crossProduct.setRight(new SelectionOperator(where, crossProductRightChild));
				}
			}
		}
		
		/* Recursively traverse the entire tree in order */
		parseTree.setLeft(decomposeAndPushDownSelects(parseTree.getLeft()));
		parseTree.setRight(decomposeAndPushDownSelects(parseTree.getRight()));
		
		return parseTree;
	}

	private static Expression mergeClauses(ArrayList<Expression> expressionList) {
		if(expressionList.isEmpty()) {
			return null;
		}
		
		if(expressionList.size() == 1) {
			return expressionList.get(0);
		}
		else {
			Expression ret = new AndExpression(expressionList.get(0), expressionList.get(1));
			for(int i=2; i<expressionList.size(); i++) {
				ret = new AndExpression(ret, expressionList.get(i));
			}
			
			return ret;
		}
	}
	
	
	
	/* Replace Selection over Cross Products with Joins */
	
	private static Operator findJoinPatternAndReplace(Operator parseTree) {
		if(parseTree == null) {
			/* Leaf Node */
			return null;
		}

		if(parseTree instanceof SelectionOperator) {
			SelectionOperator select = (SelectionOperator) parseTree;
			if(select.getLeft() != null) {
				Operator child = select.getLeft();
				if(child instanceof CrossProductOperator) {
					System.out.println("Found");
					/* TODO Pattern Matched, replace it with Join */
				}
			}
		}
		
		/* Recursively traverse the entire tree in order */
		parseTree.setLeft(findJoinPatternAndReplace(parseTree.getLeft()));
		parseTree.setRight(findJoinPatternAndReplace(parseTree.getRight()));
		
		return parseTree;
	}
	
	
	
	
	
	public static ArrayList<Expression> splitAndClauses(Expression e) {
	  
		ArrayList<Expression> ret = new ArrayList<Expression>();
		if(e instanceof AndExpression){
			AndExpression a = (AndExpression) e;
			ret.addAll(splitAndClauses(a.getLeftExpression()));
			ret.addAll(splitAndClauses(a.getRightExpression()));
		}
		else {
			ret.add(e);
		}
		
		return ret;
	}
	
}
