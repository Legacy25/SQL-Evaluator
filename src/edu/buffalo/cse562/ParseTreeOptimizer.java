package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import edu.buffalo.cse562.operators.CrossProductOperator;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.SelectionOperator;

public class ParseTreeOptimizer {

	public static void optimize(Operator parseTree) {
		/*
		 * Query plan rewrites for faster evaluation
		 */
		
		/* Replace Selection over Cross Product with appropriate Join */
		findJoinPatternAndReplace(parseTree);
		
		/* Other Patterns go here */
		
	}
	
	private static void findJoinPatternAndReplace(Operator root) {
		if(root == null) {
			/* Leaf Node */
			return;
		}

		if(root instanceof SelectionOperator) {
			if(root.getLeft() != null) {
				Operator child = root.getLeft();
				if(child instanceof CrossProductOperator) {
					/* Pattern Matched, replace it with Join */
					System.err.println("Found!");
				}
			}
		}
		
		/* Recursively traverse the entire tree in order */
		findJoinPatternAndReplace(root.getLeft());
		findJoinPatternAndReplace(root.getRight());
	}
	
	private static ArrayList<Expression> splitAndClauses(Expression e) {
	  
		ArrayList<Expression> ret = new ArrayList<Expression>();
		if(e instanceof AndExpression){
			AndExpression a = (AndExpression)e;
			ret.addAll(splitAndClauses(a.getLeftExpression()));
			ret.addAll(splitAndClauses(a.getRightExpression()));
		}
		else {
			ret.add(e);
		}
		
		return ret;
	}
	
}
