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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
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
		
		
//		File file = new File("err.txt");
//		FileOutputStream fos = null;
//		try {
//			fos = new FileOutputStream(file);
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		PrintStream ps = new PrintStream(fos);
//		System.setErr(ps);
		
		
		ArrayList<ParseTree<Operator>> parseTreeList = new ArrayList<ParseTree<Operator>>();
		
		for(File f : sqlFiles) {
			parseTreeList.add(ParseTreeGenerator.generate(dataDirs, f));
		}
		
		
		Iterator<ParseTree<Operator>> i = parseTreeList.iterator();
		while(i.hasNext()) {
			ParseTreeEvaluator.evaluate(i.next());
		}
		
	}
}
