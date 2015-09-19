/*
 * SQL Evaluator Engine
 * Authors: Arindam Nandi
 * 			Saptarshi Bhattacharjee
 * 			Sayaritra Pal
 * 
 * Spring 2015
 */

package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

import edu.buffalo.cse562.operators.Operator;

public class Main {	
	
	/* 
	 * Stores the swap directory
	 * Need application-wide access to this, so a static global
	 */
	public static File SWAP = null;
	
	/*
	 * Initialize a file name counter required for bookkeeping
	 * in external k-way merge sort
	 */
	public static int FILE_UUID = 0;
	
	/*
	 * Controls the size of the I/O buffer used to retrieve
	 * records from file to memory. Varying this gives some
	 * interesting variations in query processing time
	 */
	public static int BLOCK = 10000;
	
	/*
	 * Provides application wide access to information 
	 * regarding whether there is a memory limit,
	 * this is of use to the parse-tree optimizer
	 * in choosing non-blocking operators
	 */
	public static boolean MEMORY_LIMITED = false;
	
	/*
	 * Enabled by setting the --debug flag on the CLI.
	 * Mainly used to print out various debugging info
	 * like query plans and processing times
	 */
	public static boolean DEBUG = true;
	
	/*
	 * --fileout flag redirects the results to files
	 * instead of the console
	 */
	public static boolean FILE_OUTPUT = false;
	public static File FILE_OUTPUT_DIR = null;
	
	
	public static void main(String[] args) {
		
		DEBUG = false;
		
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
				SWAP = new File(args[i+1]);
				MEMORY_LIMITED = true;
				i++;
			}
			else if(args[i].equalsIgnoreCase("--debug")) {
				DEBUG = true;
			}
			else if(args[i].equalsIgnoreCase("--fileout")) {
				FILE_OUTPUT = true;
				FILE_OUTPUT_DIR = new File(args[i+1]);
				i++;
			}
			else {
				sqlFiles.add(new File(args[i]));
			}
		}
		
		/*
		 * Empty the swap directory of all contents so that
		 * there are no file naming conflicts
		 */
		if(SWAP != null) {
			for(File f:SWAP.listFiles()) {
				try {
					Files.deleteIfExists(f.toPath());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		/* 
		 * Keep track of query time locally.
		 * This code should be commented out before
		 * commits for submissions.
		 */
		long start = System.nanoTime();
		
		/* Generate the RA AST for each query */
		ArrayList<Operator> parseTreeList = new ArrayList<Operator>();
		for(int i = 0; i < sqlFiles.size(); i++) {
			File f = sqlFiles.get(i);
			parseTreeList.add(ParseTreeGenerator.generate(dataDirs, f));
		}
		
		/* Optimize each AST */
		for(int i = 0; i < parseTreeList.size(); i++) {
			Operator parseTree = parseTreeList.get(i);
			parseTreeList.set(i, ParseTreeOptimizer.optimize(parseTree));
		}
		
		/*
		 * Optimized query plans ready
		 */
		
		long generateTime = System.nanoTime();
		
		/* Now evaluate each parse-tree */
		for(int i=0; i< parseTreeList.size(); i++) {
			if(parseTreeList.get(i) != null) {
				if(FILE_OUTPUT) {
					ParseTreeEvaluator.output(
							parseTreeList.get(i), 
							new File(
									FILE_OUTPUT_DIR,
									sqlFiles.get(i).getName().split(".sql")[0] + ".out"
									)
							);
				} else {
					ParseTreeEvaluator.output(parseTreeList.get(i));
				}
			}
		}
		
		/* DEBUG */
		/* Show query times */
		if(Main.DEBUG) {
			
			System.err.println("GENERATE TIME: "
					+ ((double) (generateTime - start)/1000000000) 
					+ "s");
			
			System.err.println("QUERY TIME: "
					+ ((double) (System.nanoTime() - generateTime)/1000000000)
					+ "s");
		
		}
	}
}
