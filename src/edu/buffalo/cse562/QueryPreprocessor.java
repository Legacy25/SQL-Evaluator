package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.operators.ScanOperator;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class QueryPreprocessor {
	
	
	
	/* Build the indexes for all the schemas */
	public static void buildIndex(Schema s) {
		
		String file = null;
		HashMap<String, BufferedWriter> openFiles
										= new HashMap<String, BufferedWriter>();
		
		/* Secondary indices */
		for(ColumnWithType col : s.getSecondaryIndexes()) {
			file = Main.indexDirectory+"/"
							+s.getTableName()+".Secondary."
							+col.getColumnName()+".";
			
			ScanOperator scanner = new ScanOperator(s);
			scanner.initialize();
			LeafValue[] tuple = null;
			BufferedWriter bw = null;
			int pos = col.getColumnNumber();
			
			while((tuple = scanner.readOneTuple()) != null) {
				
				LeafValue key = tuple[pos];
				String keysCorrespondingFile = keyToFile(key);
				
				if(openFiles.containsKey(keysCorrespondingFile)) {
					bw = openFiles.get(keysCorrespondingFile);
				}
				else {
					try {
						bw = new BufferedWriter(new FileWriter(
								new File(file+keysCorrespondingFile+".dat"), true));
					} catch (IOException e) {
						e.printStackTrace();
					}
					openFiles.put(keysCorrespondingFile, bw);
				}
				
				try {
					bw.write(scanner.getString()+"\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			try {
				for(String key : openFiles.keySet()) {
					openFiles.get(key).flush();
					openFiles.get(key).close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			openFiles.clear();
			
			if(Main.DEBUG) {
				System.err.println("Secondary index buit for "+s.getTableName()
						+" for solumn "+col.getColumnName());
			}
		}
		
			
		/* DEBUG */
//		if(Main.DEBUG) {
//			System.err.println("Primary index buit for "+s.getTableName());
//		}
	}



	@SuppressWarnings("deprecation")
	public static String keyToFile(LeafValue key) {

		if(key instanceof LongValue) {
			try {
				long val = key.toLong();
				long ret = val % 10000;
				return String.valueOf(ret);
			} catch (InvalidLeaf e) {
				e.printStackTrace();
			}
		}
		else if(key instanceof DoubleValue) {
			try {
				double val = key.toDouble();
				long ret = (long) (val % 10000);
				return String.valueOf(ret);
			} catch (InvalidLeaf e) {
				e.printStackTrace();
			}
		}
		else if(key instanceof DateValue) {
			Date date = ((DateValue) key).getValue();
			return date.getYear()+"."+date.getMonth();
		}
		else if(key instanceof StringValue) {
			return key.toString().replace('\'', ' ').trim();
		}
		
		return null;
	}

	
	
	
	
	/*
	 * Helpers
	 */
	public static Schema generateSecondaryIndexes(Schema s) {
		String name = s.getTableName();
		ArrayList<ColumnWithType> columns = s.getColumns();
		
		switch(name) {
		case "LINEITEM":
			s.addToSecondaryIndexes(columns.get(8));
			s.addToSecondaryIndexes(columns.get(14));
			break;
		case "ORDERS":
	
			break;
		case "CUSTOMER":
			s.addToSecondaryIndexes(columns.get(6));
			break;
		case "REGION":
			s.addToSecondaryIndexes(columns.get(1));
			break;
		case "NATION":

			break;
		case "SUPPLIER":
			
			break;
		}
	
		return s;
	}
}
