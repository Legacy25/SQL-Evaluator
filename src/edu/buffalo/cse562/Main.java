/*
 * SQL Evaluator Engine
 * Authors: Arindam Nandi
 * 			Saptarshi Bhattacharjee
 * 			Sayaritra Pal
 * 
 * Spring 2015
 */

package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.function.Consumer;

import edu.buffalo.cse562.operators.NullOperator;
import edu.buffalo.cse562.operators.Operator;

public class Main {	
	
	public static final int BILLION = 1000*1000*1000;
	
	/* 
	 * Stores the swap directory
	 * Need application-wide access to this, so a static global
	 */
	public static File SWAP = null;
	
	public static ArrayList<String> DATADIRS;
	
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
	
	/*
	 * --time flag prints out query execution times to a file
	 */
	public static boolean TIME_OUTPUT = false;
	public static File TIME_OUTPUT_DIR = null;
	
	
	/*
	 * -q Quiet mode
	 */
	public static boolean QUIET = false;
	
	/*
	 * Console mode
	 */
	public static boolean IN_MEMORY = false;
	
	public static void main(String[] args) {
		
		DEBUG = false;
		
		/* Stores the data directories */
		DATADIRS = new ArrayList<String>();
		
		/* Stores the SQL files */
		ArrayList<File> sqlFiles = new ArrayList<File>();
		
		/*
		 * CLI argument parsing
		 */		
		for(int i=0; i<args.length; i++) {
			if(args[i].equalsIgnoreCase("--data")) {
				DATADIRS.add(args[i+1]);
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
			else if(args[i].equalsIgnoreCase("--time")) {
				TIME_OUTPUT = true;
				TIME_OUTPUT_DIR = new File(args[i+1]);
				i++;
			}
			else if(args[i].equals("-q")) {
				QUIET = true;
			}
			else if(args[i].equals("-mem")) {
				IN_MEMORY = true;
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
		
		String promptInput = "";
		
		System.out.println("Enter the sql files separated by a comma, enter exit to quit"
				+ "\n");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			System.out.println("> ");
			try {
				promptInput = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			String[] files = promptInput.split(",");
			for(String f : files) {
				f.trim();
				File file = new File(f);
				sqlFiles.add(file);
			}
			
			processSqlFileList(sqlFiles);
			sqlFiles.clear();
		}
	}
	
	private static void processSqlFileList(ArrayList<File> sqlFiles) {

		/* Stores query generation times for each query */
		ArrayList<Double> qgenTime = new ArrayList<Double>();
		
		/* Stores query execution times for each query */
		ArrayList<Double> qexecTime = new ArrayList<Double>();
		
		/* 
		 * Keep track of query time locally.
		 */
		long globalStart = System.nanoTime();
		
		/* Generate the RA AST for each query */
		ArrayList<Operator> parseTreeList = new ArrayList<Operator>();
		for(int i = 0; i < sqlFiles.size(); i++) {
			File f = sqlFiles.get(i);
			long localStart = System.nanoTime();
			Operator parseTree = ParseTreeGenerator.generate(DATADIRS, f);
			if(parseTree == null) {
				parseTree = new NullOperator();
			}
			parseTreeList.add(parseTree);
			
			/* Compute the generation time for this query */
			qgenTime.add((double) (System.nanoTime() - localStart)/BILLION);
		}
		
		/* Optimize each AST */
		for(int i = 0; i < parseTreeList.size(); i++) {
			Operator parseTree = parseTreeList.get(i);
			long localStart = System.nanoTime();
			parseTreeList.set(i, ParseTreeOptimizer.optimize(parseTree));

			/* Compute the optimization time for this query
			 * and add it to the generation time 
			 */
			qgenTime.set(i, qgenTime.get(i) + (double) (System.nanoTime() - localStart)/BILLION);
		}
		
		/*
		 * Display the query plans
		 */
		if(Main.DEBUG) {
			parseTreeList.forEach(new Consumer<Operator>() {
				@Override
				public void accept(Operator t) {
					System.out.println(t.getSchema());
				}
			});
		}
		
		/*
		 * Optimized query plans ready
		 */
		
		long totalGenerateTime = System.nanoTime();
		
		/* Now evaluate each parse-tree */
		for(int i=0; i< parseTreeList.size(); i++) {
			long localStart = System.nanoTime();
			
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
			
			qexecTime.add((double) (System.nanoTime() - localStart)/BILLION);
		}
		
		double totalGenTime = (double) (totalGenerateTime - globalStart)/BILLION;
		double totalExecTime = (double) (System.nanoTime() - totalGenerateTime)/BILLION;
		
		/* DEBUG */
		/* Show query times */
		if(Main.DEBUG) {
			
			System.err.println("TOTAL GENERATE TIME: "
					+ (totalGenTime) 
					+ "s");
			
			System.err.println("TOTAL EXECUTION TIME: "
					+ (totalExecTime)
					+ "s");
		
		}
		
		if(Main.TIME_OUTPUT) {
			for(int i = 0; i < sqlFiles.size(); i++) {
				try {
					if(!TIME_OUTPUT_DIR.exists()) {
						TIME_OUTPUT_DIR.mkdirs();
					}
					BufferedWriter bw = new BufferedWriter(
							new FileWriter(
									new File(
											TIME_OUTPUT_DIR,
											sqlFiles.get(i).getName().split(".sql")[0]+".stat")
									)
							);
					
					bw.write(qgenTime.get(i)+"|"+qexecTime.get(i));
					bw.flush();
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

