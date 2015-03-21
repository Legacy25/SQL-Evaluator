/*
 * SQL Evaluator Engine
 * Authors: Arindam Nandi
 * 			Saptarshi Bhattacharjee
 * 			Sayaritra Pal
 * 
 * Copyright 2015
 */

package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import edu.buffalo.cse562.operators.Operator;

public class Main {	
	
	/* 
	 * Stores the swap directory
	 * Need application-wide access to this, so a static global
	 */
	public static File swapDirectory;
	
	/*
	 * Provides application wide access to information 
	 * regarding whether there is a memory limit,
	 * this is of use to the parse-tree optimizer
	 * in choosing non-blocking operators
	 */
	public static boolean memoryLimitsOn = false;
	
	
	
	public static void main(String[] args) {
		
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			System.out.println("Press enter to continue...");
//			br.readLine();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		
		/* Detect whether limited memory is available */
		if(Runtime.getRuntime().maxMemory() / 1024 / 1024 < 100)
			memoryLimitsOn = true;
		

		/* Stores the data directories */
		ArrayList<String> dataDirs = new ArrayList<String>();
		
		/* Stores the SQL files */
		ArrayList<File> sqlFiles = new ArrayList<File>();
		
		/*
		 * CLI argument parsing
		 */		
		for(int i=0; i<args.length; i++) {
			if(args[i].equalsIgnoreCase("--data")) {
				dataDirs.add(args[i+1]);
				i++;
			}
			else if(args[i].equalsIgnoreCase("--swap")) {
				swapDirectory = new File(args[i+1]);
				i++;
			}
			else {
				sqlFiles.add(new File(args[i]));
			}
		}
		
		/* 
		 * Keep track of query time locally.
		 * This code should be commented out before
		 * commits for submissions.
		 */
//		long start = System.nanoTime();
		
		/* The generated list of parse-trees, one for each query */
		ArrayList<Operator> parseTreeList = new ArrayList<Operator>();
		for(File f : sqlFiles) {
			parseTreeList.add(ParseTreeGenerator.generate(dataDirs, f));

			/* DEBUG */
			/* Show the unoptimized Query Plan */
//			System.err.println(
//					"Unoptimized:\n\n" +
//					parseTreeList.get(parseTreeList.size()-1).getSchema()
//					);
		}
		
		/* Optimize each parse-tree */
		Iterator<Operator> i = parseTreeList.iterator();
		while(i.hasNext()) {
			Operator parseTree = i.next();
			ParseTreeOptimizer.optimize(parseTree);
			
			/* DEBUG */
			/* Show the optimized Query Plan */
//			System.err.println(
//					"Optimized:\n\n" +
//					parseTree.getSchema()
//					);
		}
		
//		long generateTime = System.nanoTime();
		
		/* Evaluate each parse-tree */
		i = parseTreeList.iterator();
		while(i.hasNext()) {
			ParseTreeEvaluator.evaluate(i.next());
		}
		
		/* DEBUG */
		/* Show query times */
//		System.err.println("\nGENERATE TIME: "+((double)(generateTime - start)/1000000000)+"s");
//		System.err.println("\nQUERY TIME: "+((double)(System.nanoTime() - generateTime)/1000000000)+"s");
		
	}
}
