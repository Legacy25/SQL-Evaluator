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
		/* Keep getting a tuple and displaying it till we exhaust the root operator */
		while((res = parseTree.readOneTuple()) != null) {
			System.out.println(display(res));
		}
		
	}
	
	public static String display(LeafValue res[]) {
		/* Formatting logic */
		boolean flag = false;
		String result = "";
		
		for(int i=0; i<res.length; i++) {
			if(flag)
				result += "|";
			
			if(!flag)
				flag = true;
			
			if(res[i] instanceof StringValue) {
				String str = res[i].toString();
				result += str.substring(1, str.length() - 1);				
			}
			else if(res[i] instanceof DoubleValue) {
				DecimalFormat twoDForm = new DecimalFormat("#.####");
			    try {
			    	result += twoDForm.format(res[i].toDouble());
				} catch (NumberFormatException | InvalidLeaf e) {
					e.printStackTrace();
				}
				
			}
			else {				
				result += res[i].toString();
			}
		}
		
		return result;
	}

}
