package edu.buffalo.cse562;

import java.text.DecimalFormat;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
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
		
		LeafValue res[] = null;
		StringBuilder output = new StringBuilder(10000);
		/* Keep getting a tuple and displaying it till we exhaust the root operator */
		while((res = parseTree.readOneTuple()) != null) {
			output.append(display(res));
		}
		
		System.out.println(output);
	}
	
	public static StringBuilder display(LeafValue res[]) {
		/* Formatting logic */
		boolean flag = false;
		StringBuilder result = new StringBuilder(50);
		
		for(int i=0; i<res.length; i++) {
			if(flag)
				result.append("|");
			
			if(!flag)
				flag = true;
			
			if(res[i] instanceof StringValue) {
				String str = res[i].toString();
				result.append(str.substring(1, str.length() - 1));				
			}
			else if(res[i] instanceof DoubleValue) {
				DecimalFormat twoDForm = new DecimalFormat("#.####");
			    try {
			    	result.append(twoDForm.format(res[i].toDouble()));
				} catch (NumberFormatException | InvalidLeaf e) {
					e.printStackTrace();
				}
				
			}
			else {				
				result.append(res[i].toString());
			}
		}
		
		return result.append('\n');
	}

}
