package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.datastructures.ParseTree;
import edu.buffalo.cse562.exceptions.InsertOnNonEmptyBranchException;
import edu.buffalo.cse562.exceptions.UnsupportedStatementException;
import edu.buffalo.cse562.operators.JoinOperator;
import edu.buffalo.cse562.operators.Operator;
import edu.buffalo.cse562.operators.ProjectionOperator;
import edu.buffalo.cse562.operators.ScanOperator;
import edu.buffalo.cse562.operators.SelectionOperator;
import edu.buffalo.cse562.schema.ColumnWTyp;
import edu.buffalo.cse562.schema.Schema;

public class ParseTreeGenerator {

	// The tables HashMap keeps a mapping of tables to their corresponding schemas
	private static HashMap<String, Schema> tables = new HashMap<String, Schema>();


	
	
	// Function to find a table within the provided Data Directories
	private static String findFile(ArrayList<String> dataDirs, String tableName) {
		for(String dDirs : dataDirs) {
			File dir = new File(dDirs);
			File files[] = dir.listFiles();
			for(File f : files) {
				if(f.getName().equalsIgnoreCase(tableName+".dat"))
					return f.getAbsolutePath();
			}
		}
		return null;
	}
	
	
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ParseTree<Operator> generate(ArrayList<String> dataDirs, File sqlFile) {
		
		ParseTree<Operator> parseTree = new ParseTree<Operator>();

		Statement statement = null;
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(sqlFile));

			while((statement = parser.Statement()) != null) {

				/*
				 * CREATE TABLE
				 */
				
				if(statement instanceof CreateTable) {
					CreateTable cTable = (CreateTable) statement;
					String tableName = cTable.getTable().toString();
					String tableFile = findFile(dataDirs, tableName);
					
					if(tableFile == null) {
						System.err.println("Table "+ tableName + " not found in any "
								+ "of the specified directories!");
						System.exit(1);
					}
					Schema schema = new Schema(tableName, tableFile);
					Iterator i = cTable.getColumnDefinitions().listIterator();
					while(i.hasNext()) {
						String colNameAndType[] = i.next().toString().split(" ");
						ColumnWTyp c = new ColumnWTyp(cTable.getTable(), colNameAndType[0], colNameAndType[1]);
						schema.addColumn(c);
					}
					tables.put(tableName, schema);
				}
				
		
				/*
				 * SELECT
				 */
				
				
				else if(statement instanceof Select) {
					SelectBody body = ((Select) statement).getSelectBody();
					if(body instanceof PlainSelect) {
						PlainSelect ps = (PlainSelect) body;
						Schema schema = tables.get(ps.getFromItem().toString());
						parseTree.insertRoot(new ScanOperator(schema));
						
						if(ps.getJoins() != null){
							Iterator i = ps.getJoins().iterator();
							while(i.hasNext()) {
								Join join = (Join) i.next();
								
								ParseTree<Operator> right = new ParseTree<Operator>(
										new ScanOperator(tables.get(join.getRightItem().toString())));
								parseTree.insertRoot(new JoinOperator(parseTree.getLeft().getRoot(),
										right.getRoot()));
								parseTree.insertBranch(right, ParseTree.Side.RIGHT);
								
								if(join.getOnExpression() != null) {
									parseTree.insertRoot(new SelectionOperator(join.getOnExpression(), parseTree.getLeft().getRoot()));										
								}
							}
						}
						
						if(ps.getWhere() != null) {
							parseTree.insertRoot(new SelectionOperator(ps.getWhere(), parseTree.getLeft().getRoot()));
						
						}
						
						if(ps.getSelectItems() != null) {
							parseTree.insertRoot(new ProjectionOperator(ps.getSelectItems(), parseTree.getLeft().getRoot()));
						}
							
					}
					else if(body instanceof Union) {
						//TODO Union
					}
				}
				
				/*
				 * Unsupported Statement
				 */
				
				else {
					throw new UnsupportedStatementException();
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println("File "+sqlFile+" not found!");
		} catch (ParseException e) {
			System.err.println("Parse Exception");
		} catch (UnsupportedStatementException e) {
			System.err.println("Unsupported SQL Statement");
		} catch (InsertOnNonEmptyBranchException e) {
			System.err.println("Tried to insert on a non-empty branch");
		}
		
		return parseTree;
		
	}
	

	
	public static HashMap<String, Schema> getTableSchemas() {
		return tables;
	}
}
