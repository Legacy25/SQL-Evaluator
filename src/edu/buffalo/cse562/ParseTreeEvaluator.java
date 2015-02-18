package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.datastructures.ParseTree;
import edu.buffalo.cse562.operators.Operator;

public class ParseTreeEvaluator {

	public static void evaluate(ParseTree<Operator> parseTree) {
		if(parseTree == null)
			return;
		
		if(parseTree.getRoot() == null)
			return;
		
		LeafValue res[];
		while((res = parseTree.getRoot().readOneTuple()) != null) {
			display(res);
		}
	}
	
	private static void display(LeafValue res[]) {
		
		boolean flag = false;
		
		for(int i=0; i<res.length; i++) {
			if(flag)
				System.out.print("|");
			
			if(!flag)
				flag = true;
			
			System.out.print(res[i]);
		}
		
		System.out.println();
	}
}
