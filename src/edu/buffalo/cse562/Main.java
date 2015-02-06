/*
 * SQL Evaluator Engine
 * Authors: Arindam Nandi
 * 			Saptarshi Bhattacharjee
 * 			Sayaritra Pal
 * 
 * Copyright 2015
 */

package edu.buffalo.cse562;

public class Main {
	
	public static final int INVALID_ARGUMENTS = 1;
	
	public static void main(String[] args) {

		/*
		 * CLI argument parsing code
		 */
		
		if(args.length < 3) {
			System.err.println("Usage: Main.java --data [DATA FOLDER] sql_file(s).sql");
			System.exit(INVALID_ARGUMENTS);
		}

		
		
	}

}
