package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.operators.Operator;

public class ParseTreeEvaluator {

	/*
	 * Evaluates a parse-tree
	 */
	public static void evaluate(Operator parseTree) {

		if(parseTree == null) {
			return;
		}
		
		parseTree.initialize();
		
		LeafValue res[];
		StringBuilder output = new StringBuilder();
		
		/* Keep getting a tuple and displaying it till we exhaust the root operator */
		while((res = parseTree.readOneTuple()) != null) {
			output.append(display(res));
		}
		
		System.out.println(output);
	}
	
	public static String display(LeafValue res[]) {
		/* Formatting logic */
		boolean flag = false;
		StringBuilder output = new StringBuilder();
		
		for(int i=0; i<res.length; i++) {
			if(flag)
				output.append("|");
			
			if(!flag)
				flag = true;
			
			if(res[i] instanceof StringValue) {
				String str = res[i].toString();
				output.append(str.substring(1, str.length() - 1));				
			}
			else {				
				output.append(res[i]);
			}
		}
		
		output.append("\n");
		
		return output.toString();
	}
}
