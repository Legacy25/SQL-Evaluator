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

		ArrayList<String> dataDirs = new ArrayList<String>();
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
		
		ArrayList<ParseTree<Operator>> parseTreeList = new ArrayList<ParseTree<Operator>>();
		
//		Long start = System.nanoTime();
		
		for(File f : sqlFiles) {
			parseTreeList.add(ParseTreeGenerator.generate(dataDirs, f));
		}
		
		
		Iterator<ParseTree<Operator>> i = parseTreeList.iterator();
		while(i.hasNext()) {
			ParseTreeEvaluator.evaluate(i.next());
		}
		
//		Long end = System.nanoTime();
//		System.out.println((double)(end - start) / 1000000000);
	}
}
