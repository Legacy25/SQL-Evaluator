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
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

public class Main {	
	
	public static void main(String[] args) {

		ArrayList<File> dataFiles = new ArrayList<File>();
		ArrayList<File> sqlFiles = new ArrayList<File>();
		HashMap<String, CreateTable> tables = new HashMap<String, CreateTable>();
		
		
		/*
		 * CLI argument parsing code
		 */
		
		for(int i=0; i<args.length; i++) {
			if(args[i].equals("--data")) {
				dataFiles.addAll(0, new ArrayList<File>(Arrays.asList(new File(args[i+1]).listFiles())));
				i++;
			}
			else {
				sqlFiles.add(new File(args[i]));
			}
		}
		
		System.out.println("Datafiles: "+dataFiles);
		System.out.println("Sqlfiles: "+sqlFiles);
		
		
		
		/*
		 * Parsing Code
		 */
		
		Statement statement = null;
		for(File f : sqlFiles){
			try {
				CCJSqlParser parser = new CCJSqlParser(new FileReader(f));

				while((statement = parser.Statement()) != null) {
					if(statement instanceof CreateTable) {
						//TODO
						System.out.println(statement);
					}
					else if(statement instanceof Select) {
						SelectBody body = ((Select) statement).getSelectBody();
						if(body instanceof PlainSelect) {
							//TODO PlainSelect
							System.out.println(body);
						}
						else if(body instanceof Union) {
							//TODO Union
							System.out.println(body);
						}
					}
				}
			} catch (FileNotFoundException e) {
				System.err.println("File "+f+" not found!");
			} catch (ParseException e) {
				System.err.println("Parse Exception");
			}
		}		
	}
}
