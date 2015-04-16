/*
 * SQL Evaluator Engine
 * Authors: Arindam Nandi
 * 			Saptarshi Bhattacharjee
 * 			Sayaritra Pal
 * 
 * Copyright 2015
 */

package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.schema.Schema;

public class Main {	
	
	/* 
	 * Stores the swap directory
	 * Need application-wide access to this, so a static global
	 */
	public static File swapDirectory = null;
	
	public static File indexDirectory = null;
	
	public static int fileUUID = 0;
	
	public static int BLOCK = 10000;
	
	/*
	 * Provides application wide access to information 
	 * regarding whether there is a memory limit,
	 * this is of use to the parse-tree optimizer
	 * in choosing non-blocking operators
	 */
	public static boolean memoryLimitsOn = false;
	public static boolean preprocessingOn = false;
	
	private static final String createTableStatementsFile = "schema.sql";
	
	
	
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			System.out.println("Press enter to continue...");
//			br.readLine();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		
//		/* Detect whether limited memory is available */
//		if(Runtime.getRuntime().maxMemory() / 1024 / 1024 < 100)
//			memoryLimitsOn = true;
		

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
				memoryLimitsOn = true;
				i++;
			}
			else if(args[i].equalsIgnoreCase("--db")) {
				indexDirectory = new File(args[i+1]);
				i++;
			}
			else if(args[i].equalsIgnoreCase("--load")) {
				preprocessingOn = true;
			}
			else {
				sqlFiles.add(new File(args[i]));
			}
		}
		
		if(swapDirectory != null) {
			for(File f:swapDirectory.listFiles()) {
				try {
					Files.deleteIfExists(f.toPath());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		if(preprocessingOn) {
			for(File f : sqlFiles) {
				ParseTreeGenerator.generate(dataDirs, f);
			}
			
			ArrayList<Schema> tables = ParseTreeGenerator.getTableSchemas();
			
			for(Schema s:tables) {
				QueryPreprocessor.buildIndex(s);
				System.err.println("Index built for "+s.getTableName());
			}
			
			BufferedWriter bw = null;
			try {
				bw = new BufferedWriter(new FileWriter(indexDirectory+"//"+createTableStatementsFile));
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			ArrayList<String> createTableStatements = ParseTreeGenerator.getCreateTableStatements();
			for(String ctStatement : createTableStatements) {
				try {
					bw.write(ctStatement+"\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			try {
				bw.flush();
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		
		sqlFiles.add(0, new File(indexDirectory+"//"+createTableStatementsFile));
		
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
		for(int i=0; i< parseTreeList.size(); i++) {
			Operator parseTree = parseTreeList.get(i);
			parseTreeList.set(i, ParseTreeOptimizer.optimize(parseTree));
		}
		
//		long generateTime = System.nanoTime();
		
		/* Evaluate each parse-tree */
		for(int i=0; i< parseTreeList.size(); i++) {
			
			/* DEBUG */
			/* Show the optimized Query Plan */
//			System.err.println(
//					"Optimized:\n\n" +
//					parseTreeList.get(i).getSchema()
//					);
			
			ParseTreeEvaluator.evaluate(parseTreeList.get(i));
		}
		
		/* DEBUG */
		/* Show query times */
//		System.err.println("\nGENERATE TIME: "+((double)(generateTime - start)/1000000000)+"s");
//		System.out.println("\nQUERY TIME: "+((double)(System.nanoTime() - generateTime)/1000000000)+"s");
		
	}
}
