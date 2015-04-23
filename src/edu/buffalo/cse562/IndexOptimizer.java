package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.operators.IndexEqualityProjectScanOperator;
import edu.buffalo.cse562.operators.IndexRangeScanOperator;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.ProjectScanOperator;
import edu.buffalo.cse562.operators.SelectionOperator;
import edu.buffalo.cse562.schema.Schema;

public class IndexOptimizer {
	
	public static Operator optimize(Operator parseTree) {
		if(parseTree == null) {
			return null;
		}
		
		if(Main.indexDirectory == null) {
			return parseTree;
		}
		
		parseTree = replaceSelectionScansWithIndexScans(parseTree);
		
		parseTree = replaceSelectionScansWithIndexRangeScans(parseTree);
		
		/* Generate appropriate table names after optimization */
		parseTree.generateSchemaName();
		
		return parseTree;
	}

	private static Operator replaceSelectionScansWithIndexRangeScans(
			Operator parseTree) {
		
		if(parseTree == null)
			return null;
		
		if(parseTree instanceof SelectionOperator) {
			Operator child = parseTree.getLeft();
			if(child instanceof ProjectScanOperator) {
				Expression where = ((SelectionOperator) parseTree).getWhere();
				Column chosenColumn = null;
				ArrayList<Expression> clauseList = ParseTreeOptimizer.splitAndClauses(where);
				ArrayList<Expression> chosenList = new ArrayList<Expression>();
				
				for(Expression clause : clauseList) {
					if(!(clause instanceof BinaryExpression)) {
						continue;
					}

					BinaryExpression beClause = (BinaryExpression) clause;
					Column col = getClauseColumn(beClause);
					
					if(!literalClause(beClause)) {
						continue;
					}
					
					if(!isASecondaryIndex(col, child.getSchema())) {
						continue;
					}
					
					if(!inequalityClause(beClause)) {
						continue;
					}
					
					if(chosenColumn != null 
							&& !col.getColumnName().equalsIgnoreCase(chosenColumn.getColumnName())) {
						
						continue;
					}
					
					chosenColumn = col;
					chosenList.add(clause);
				}
				
				if(chosenList.size() > 0) {
					parseTree.setLeft(new IndexRangeScanOperator(
							((ProjectScanOperator) child).getOldSchema(),
							child.getSchema(),
							chosenList,
							chosenColumn
							));
				}
			}
		}
		
		parseTree.setLeft(replaceSelectionScansWithIndexRangeScans(parseTree.getLeft()));
		parseTree.setRight(replaceSelectionScansWithIndexRangeScans(parseTree.getRight()));
		
		return parseTree;
	}

	private static Column getClauseColumn(BinaryExpression clause) {
		return (Column) clause.getLeftExpression();
	}

	private static boolean inequalityClause(BinaryExpression clause) {
		if(clause instanceof GreaterThan || clause instanceof GreaterThanEquals
				|| clause instanceof MinorThan || clause instanceof MinorThanEquals) {
			return true;
		}
		return false;
	}

	private static boolean literalClause(BinaryExpression clause) {
		if(clause.getRightExpression() instanceof LeafValue 
				|| clause.getRightExpression() instanceof Function) {
			return true;
		}
		
		return false;
	}

	private static Operator replaceSelectionScansWithIndexScans(
			Operator parseTree) {
		
		if(parseTree == null)
			return null;
		
		if(parseTree instanceof SelectionOperator) {
			SelectionOperator selOp = (SelectionOperator) parseTree;
			Operator child = parseTree.getLeft();
			if(child instanceof ProjectScanOperator) {
				ProjectScanOperator psOp = (ProjectScanOperator) child;
				
				ArrayList<Expression> indexExists = new ArrayList<Expression>();
				ArrayList<Expression> noIndex = new ArrayList<Expression>();
				
				Expression where = selOp.getWhere();
				ArrayList<Expression> clauseList = ParseTreeOptimizer.splitAndClauses(where);
				for(Expression e : clauseList) {
					if(indexExists(e, child.getSchema())) {
						indexExists.add(e);
					}
					else {
						noIndex.add(e);
					}
				}
				
				getLookupConditions(indexExists, noIndex);
				where = ParseTreeOptimizer.mergeClauses(noIndex);
				Expression indexWhere = ParseTreeOptimizer.mergeClauses(indexExists);
				
				if(where != null) {
					selOp.setWhere(where);
					if(indexWhere != null) {
						parseTree.setLeft(
								new IndexEqualityProjectScanOperator(
										psOp.getOldSchema(), 
										psOp.getSchema(), 
										indexWhere
								)
						);
					}
				}

				else {
					if(indexWhere != null) {
						parseTree = 
								new IndexEqualityProjectScanOperator(
										psOp.getOldSchema(), 
										psOp.getSchema(), 
										indexWhere
								);
					}			
				}
			}
		}
		
		parseTree.setLeft(replaceSelectionScansWithIndexScans(parseTree.getLeft()));
		parseTree.setRight(replaceSelectionScansWithIndexScans(parseTree.getRight()));
		
		return parseTree;
	}

	private static void getLookupConditions(ArrayList<Expression> indexExists,
			ArrayList<Expression> noIndex) {
		
		if(indexExists.size() == 0) {
			return;
		}
		
		String chosenCol = chooseColumn(indexExists);
		ArrayList<Expression> tempList = new ArrayList<Expression>();
		
		for(Expression e : indexExists) {
			Column col = null;
			if(e instanceof Parenthesis) {
				Parenthesis p = (Parenthesis) e;
				BinaryExpression b = (BinaryExpression) ParseTreeOptimizer.splitOrClauses(p.getExpression()).get(0);
				col = (Column) (b.getLeftExpression());
			}
			else {
				col = (Column) ((BinaryExpression) e).getLeftExpression();
			}
			if(! col.getColumnName().equalsIgnoreCase(chosenCol)) {
				noIndex.add(e);
				tempList.add(e);
			}
		}
		
		for(Expression e : tempList) {
			indexExists.remove(e);
		}
		
	}

	private static String chooseColumn(ArrayList<Expression> indexExists) {
		// TODO Cost Based Optimization goes here
		
		Expression e = indexExists.get(0);
		ArrayList<Expression> orClauses;
		
		if(e instanceof Parenthesis) {
			orClauses = ParseTreeOptimizer.splitOrClauses(((Parenthesis) e).getExpression());
		}
		else {
			orClauses = new ArrayList<Expression>();
			orClauses.add(e);
		}
		
		return ((Column) ((BinaryExpression) orClauses.get(0)).getLeftExpression()).getColumnName();
	}

	private static boolean indexExists(Expression e, Schema schema) {
		ArrayList<Expression> orClauses;
		
		if(e instanceof Parenthesis) {
			orClauses = ParseTreeOptimizer.splitOrClauses(((Parenthesis) e).getExpression());
		}
		else {
			orClauses = new ArrayList<Expression>();
			orClauses.add(e);
		}
		for(Expression exp : orClauses) {
			if(exp instanceof BinaryExpression) {
				if(!(exp instanceof EqualsTo)) {
					return false;
				}
				
				Expression left = ((BinaryExpression) exp).getLeftExpression();
				Expression right = ((BinaryExpression) exp).getRightExpression();
				
				if(left instanceof Column) {
					if(!(isAPrimaryKey((Column) left, schema)
							|| isASecondaryIndex((Column) left, schema))) {
						
						return false;
					}
				}
				if(right instanceof Column) {
					return false;
				}
			}
		}
		
		return true;
	}

	public static boolean isAPrimaryKey(Column left, Schema schema) {

		if(schema.getPrimaryKeySize() != 1) {
			return false;
		}
		
		if(schema.getPrimaryKey(0).getColumnName().equalsIgnoreCase(left.getColumnName())) {
			return true;
		}
		
		return false;
	}

	public static boolean isASecondaryIndex(Column left, Schema schema) {
		
		if(schema.getSecondaryIndexes().size() == 0) {
			return false;
		}
		
		for(Column col : schema.getSecondaryIndexes()) {
			if(col.getColumnName().equalsIgnoreCase(left.getColumnName())) {
				return true;
			}
		}
		
		return false;
	}

}
