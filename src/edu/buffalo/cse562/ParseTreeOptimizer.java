package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.operators.CrossProductOperator;
import edu.buffalo.cse562.operators.ExternalHashJoinOperator;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.SelectionOperator;
import edu.buffalo.cse562.schema.Schema;

public class ParseTreeOptimizer {

	private enum ClauseApplicability {
		ROOT, LEFT, RIGHT, INVALID;
	}
	
	/* 
	 * The entry-point to the optimizer,
	 * we try to define several functions which
	 * apply a variety of rewriting techniques to optimize
	 * the query plan
	 */
	public static void optimize(Operator parseTree) {
		
		if(parseTree == null) {
			return;
		}
		
		/*
		 * Query plan rewrites for faster evaluation
		 */
		
		/* Decompose Select Clauses and push them down appropriately */
		parseTree = initialSelectionDecomposition(parseTree);
		
		/* Replace Selection over Cross Product with appropriate Join */
		parseTree = findJoinPatternAndReplace(parseTree);
		
//		/* Generate appropriate table names after optimization */
		parseTree.generateSchemaName();
		
		/* Reorder Cross Products to facilitate more joins */
		parseTree = reOrderCrossProducts(parseTree);
		
		/* Other Patterns go here */
		
		/* Generate appropriate table names after optimization */
		parseTree.generateSchemaName();
		
	}
	
	private static Operator reOrderCrossProducts(Operator parseTree) {
		if(parseTree == null) {
			return null;
		}
		
		
		/* Recursively traverse the entire tree bottom up */
		parseTree.setLeft(reOrderCrossProducts(parseTree.getLeft()));
		parseTree.setRight(reOrderCrossProducts(parseTree.getRight()));
		
		
		if(parseTree instanceof SelectionOperator) {
			SelectionOperator select = (SelectionOperator) parseTree;
			Expression where = select.getWhere();
			
			if(isJoinPredicate(where)) {
				if(select.getLeft() instanceof ExternalHashJoinOperator) {
					Operator cpOperatorParent = findCrossProduct(select);
					if(cpOperatorParent != null) {
						CrossProductOperator cpOperator = 
								(CrossProductOperator) cpOperatorParent.getLeft();
						
						Operator newCrossProduct = null;
						
						if(relationIsLeftOfCrossProduct(where, cpOperator)) {
							cpOperatorParent.setLeft(cpOperator.getRight());
							
							newCrossProduct = new CrossProductOperator(
									select.getLeft() ,
									cpOperator.getLeft()
									);
						}
						else {
							cpOperatorParent.setLeft(cpOperator.getLeft());
							
							newCrossProduct = new CrossProductOperator(
									select.getLeft() ,
									cpOperator.getRight()
									);
						}

						
						parseTree.setLeft(newCrossProduct);
						parseTree = findJoinPatternAndReplace(parseTree);
					}
				}
			}
		}
		
		return parseTree;
	}
	
	private static boolean relationIsLeftOfCrossProduct(Expression where,
			CrossProductOperator cpOperator) {

		BinaryExpression joinClause = (BinaryExpression) where;
		Column leftColumn = (Column) joinClause.getLeftExpression();
		Schema leftSchema = cpOperator.getLeft().getSchema();
		Schema rightSchema = cpOperator.getRight().getSchema();

		if(belongsToSchema(leftSchema, rightSchema, leftColumn) 
				== ClauseApplicability.INVALID) {
			return false;
		}
		
		return true;
	}

	private static Operator findCrossProduct(
			Operator o) {
		if(o == null)
			return null;
		
		if(o.getLeft() instanceof CrossProductOperator)
			return o;
		
		Operator cpOperator = findCrossProduct(o.getLeft());
		if(cpOperator == null)
			return findCrossProduct(o.getLeft());
		else
			return cpOperator;
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
					 */
					return ClauseApplicability.ROOT;
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
		else if(clause instanceof Parenthesis) {
			Parenthesis parenClause = (Parenthesis) clause;
			Expression parenExpr = parenClause.getExpression();

			if(parenExpr instanceof OrExpression) {
				ArrayList<Expression> orClauseList = splitOrClauses(parenExpr);
				
				if(orClauseList.isEmpty()) {
					/* Safety check */
					return ClauseApplicability.INVALID;
				}
				
				ClauseApplicability firstCa = checkClauseApplicability(left, right, orClauseList.get(0));
				
				for(int i=1; i<orClauseList.size(); i++) {
					if(checkClauseApplicability(left, right, orClauseList.get(i)) != firstCa)
						return ClauseApplicability.ROOT;
				}
				
				return firstCa;
			}
		}
		
		return ClauseApplicability.INVALID;
		
	}
	
	public static ArrayList<Expression> splitOrClauses(Expression e) {
		ArrayList<Expression> ret = new ArrayList<Expression>();
		if(e instanceof OrExpression){
			OrExpression a = (OrExpression) e;
			ret.addAll(splitOrClauses(a.getLeftExpression()));
			ret.addAll(splitOrClauses(a.getRightExpression()));
		}
		else {
			ret.add(e);
		}
		
		return ret;
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

	
	
	private static Operator initialSelectionDecomposition(Operator parseTree) {
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
				ArrayList<Expression> invalidList = new ArrayList<Expression>();
				
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
					case INVALID:
						invalidList.add(clause);
						break;
					}
				}

				
				if(!leftList.isEmpty()) {
					Expression where = mergeClauses(leftList);
					crossProduct.setLeft(new SelectionOperator(where, crossProductLeftChild));
				}
				
				if(!rightList.isEmpty()) {
					Expression where = mergeClauses(rightList);
					crossProduct.setRight(new SelectionOperator(where, crossProductRightChild));
				}
				
				if(!parentList.isEmpty()) {
					Expression where = mergeClauses(parentList);
					parseTree = new SelectionOperator(where, crossProduct);
				}
				else {
					parseTree = crossProduct;
				}
				
				if(!invalidList.isEmpty()) {
					Expression where = mergeClauses(invalidList);
					parseTree = new SelectionOperator(where, parseTree);
				}
			}
		}
		
		/* Recursively traverse the entire tree in order */
		parseTree.setLeft(initialSelectionDecomposition(parseTree.getLeft()));
		parseTree.setRight(initialSelectionDecomposition(parseTree.getRight()));
		
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
			
			Operator child = select.getLeft();

			if(child instanceof CrossProductOperator) {
				/* Pattern Matched, replace it with Join */
				Operator leftChild = child.getLeft();
				Operator rightChild = child.getRight();
				
				Expression where = select.getWhere();

				ArrayList<Expression> joinPredicates = new ArrayList<Expression>();
				ArrayList<Expression> clauseList = new ArrayList<Expression>();
				
				if(where instanceof AndExpression) {
					clauseList = splitAndClauses(where);
					
					for(int i=0; i<clauseList.size(); i++) {
						Expression clause = clauseList.get(i);
						if(isJoinPredicate(clause)
								//&& consistent(clause, joinPredicates)
								) {
							joinPredicates.add(clause);
							clauseList.remove(clause);
						}
					}
				}
				else {
					joinPredicates.add(where);
				}
				
				/* Pretty sure that joinPredicates will have at least
				 * one member, but we still check for it
				 */
				
				if(!clauseList.isEmpty()) {
					parseTree = new SelectionOperator(
							mergeClauses(clauseList),
							parseTree
							);
					if(!joinPredicates.isEmpty()) {
						parseTree.setLeft(new ExternalHashJoinOperator(
							mergeClauses(joinPredicates) ,
							leftChild ,
							rightChild
							)
						);
					}
				} 
				else if(!joinPredicates.isEmpty()) {
					parseTree = new ExternalHashJoinOperator(
							mergeClauses(joinPredicates) ,
							leftChild ,
							rightChild
							);
				}
			}
		}
		
		/* Recursively traverse the entire tree in order */
		parseTree.setLeft(findJoinPatternAndReplace(parseTree.getLeft()));
		parseTree.setRight(findJoinPatternAndReplace(parseTree.getRight()));
		
		return parseTree;
	}
	
	
	
	
	
//	private static boolean consistent(Expression clause,
//			ArrayList<Expression> joinPredicates) {
//
//		if(joinPredicates.isEmpty())
//			return true;
//		
//		HashSet<String> joinTables = new HashSet<String>();
//		BinaryExpression firstPredicate = (BinaryExpression) joinPredicates.get(0);
//		BinaryExpression bClause = (BinaryExpression) clause;
//		
//		joinTables.add(((Column) firstPredicate.getLeftExpression()).getTable().getWholeTableName());
//		joinTables.add(((Column) firstPredicate.getRightExpression()).getTable().getWholeTableName());
//		
//		if(joinTables.contains(((Column) bClause.getLeftExpression()).getTable().getWholeTableName())
//				&&
//				joinTables.contains(((Column) bClause.getRightExpression()).getTable().getWholeTableName())
//				)
//			return true;
//		
//		return false;
//	}

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
	
	public static Boolean isJoinPredicate(Expression e) {
		
		if(e instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression) e;
			if(be.toString().contains("=")
					&& be.getLeftExpression() instanceof Column
					&& be.getRightExpression() instanceof Column)
				
				return true;
		}
		
		return false;
	}
}
