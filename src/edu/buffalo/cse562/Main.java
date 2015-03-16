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

import edu.buffalo.cse562.datastructures.ParseTree;
import edu.buffalo.cse562.operators.Operator;

public class Main {	

	public static void main(String[] args) {

		/* Stores the data directories */
		ArrayList<String> dataDirs = new ArrayList<String>();
		
		/* Stores the SQL files */
		ArrayList<File> sqlFiles = new ArrayList<File>();
		
		/*
		 * CLI argument parsing
		 */		
		for(int i=0; i<args.length; i++) {
			if(args[i].equals("--data")) {
				dataDirs.add(args[i+1]);
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
		long start = System.nanoTime();
		
		/* The generated list of parse-trees, one for each query */
		ArrayList<ParseTree<Operator>> parseTreeList = new ArrayList<ParseTree<Operator>>();
		for(File f : sqlFiles) {
			parseTreeList.add(ParseTreeGenerator.generate(dataDirs, f));
		}
		
		long generateTime = System.nanoTime();
		
		/* Evaluate each parse-tree */
		Iterator<ParseTree<Operator>> i = parseTreeList.iterator();
		while(i.hasNext()) {
			ParseTreeEvaluator.evaluate(i.next());
		}
		
		/* Show query times */
		System.out.println("GNERATE TIME: "+((double)(generateTime - start)/1000000000)+"s");
		System.out.println("QUERY TIME: "+((double)(System.nanoTime() - generateTime)/1000000000)+"s");
		
	}
}
