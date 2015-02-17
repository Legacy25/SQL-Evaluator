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
import java.util.Iterator;

import edu.buffalo.cse562.datastructures.ParseTree;
import edu.buffalo.cse562.exceptions.InsertOnNonEmptyBranchException;
import edu.buffalo.cse562.exceptions.UnsupportedStatementException;
import edu.buffalo.cse562.operators.JoinOperator;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.ProjectionOperator;
import edu.buffalo.cse562.operators.ScanOperator;
import edu.buffalo.cse562.operators.SelectionOperator;
import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

public class Main {	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {

		ArrayList<File> dataFiles = new ArrayList<File>();
		ArrayList<File> sqlFiles = new ArrayList<File>();
		HashMap<String, Schema> tables = new HashMap<String, Schema>();
		ParseTree<Operator> parseTree = new ParseTree<Operator>();
		
		
		/*
		 * CLI argument parsing
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
		
		
		
		/*
		 * SQL parsing
		 */
		
		Statement statement = null;
		for(File f : sqlFiles){
			try {
				CCJSqlParser parser = new CCJSqlParser(new FileReader(f));

				while((statement = parser.Statement()) != null) {

					if(statement instanceof CreateTable) {
						CreateTable cTable = (CreateTable) statement;
						String tableName = cTable.getTable().toString();
						Schema schema = new Schema(tableName, "data/"+tableName.toLowerCase()+".dat");
						tables.put(tableName, schema);
					}
					
					else if(statement instanceof Select) {
						SelectBody body = ((Select) statement).getSelectBody();
						if(body instanceof PlainSelect) {
							PlainSelect ps = (PlainSelect) body;
							Schema schema = tables.get(ps.getFromItem().toString());
							parseTree.insertRoot(new ScanOperator(schema.getTableFile()));
							
							if(ps.getJoins() != null){
								Iterator i = ps.getJoins().iterator();
								while(i.hasNext()) {
									ParseTree<Operator> right = new ParseTree<Operator>(
											new ScanOperator(tables.get(i.next().toString()).getTableFile()));

									parseTree.insertRoot(new JoinOperator(parseTree.getLeft().getRoot(),
											right.getRoot()));
									
									parseTree.insertBranch(right, ParseTree.Side.RIGHT);
								}
							}
							
							if(ps.getWhere() != null)
								parseTree.insertRoot(new SelectionOperator(ps.getWhere(), parseTree.getLeft().getRoot()));
							
							if(ps.getSelectItems() != null) {
								System.out.println(ps.getSelectItems().get(0));
								parseTree.insertRoot(new ProjectionOperator(ps.getSelectItems(), parseTree.getLeft().getRoot()));
							}
								
						}
						else if(body instanceof Union) {
							//TODO Union
						}
					}
					
					else {
						throw new UnsupportedStatementException();
					}
				}
			} catch (FileNotFoundException e) {
				System.err.println("File "+f+" not found!");
			} catch (ParseException e) {
				System.err.println("Parse Exception");
			} catch (UnsupportedStatementException e) {
				System.err.println("Unsupported SQL Statement");
			} catch (InsertOnNonEmptyBranchException e) {
				System.err.println("Tried to insert on a non-empty branch");
			}
		}
		
		evaluate(parseTree);
	}
	
	public static void evaluate(ParseTree<Operator> parseTree) {
		if(parseTree == null)
			return;
		
		if(parseTree.getRoot() == null)
			return;
		
		Long res[];
		while((res = parseTree.getRoot().readOneTuple()) != null) {
			display(res);
		}
	}
	
	public static void display(Long res[]) {
		
		boolean flag = false;
		
		for(int i=0; i<res.length; i++) {
			if(flag)
				System.out.print("|");
			
			if(!flag)
				flag = true;
			
			System.out.print(res[i]);
		}
		
		System.out.println();
	}
}
