package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.schema.Schema;


/*
 * The ScanOperator is used to scan a file and return one row at a time
 * The file to be read is known from the Schema
 */
public class ScanOperator implements Operator {

	private Schema schema;
	private BufferedReader br;
	private File f;
	
	
	
	public ScanOperator(Schema schema) {		
		this.schema = schema;
		try {
			f = new File(schema.getTableFile());
		} catch (NullPointerException e) {
			System.err.println("Could not find schema.");
			e.printStackTrace();
		}
		br = null;
		reset();
		
	}
	
	
	
	@Override
	public LeafValue[] readOneTuple() {
		if(br == null) {
			return null;
		}

		String line = null;
		try {
			if((line = br.readLine()) == null) {
				return null;
			}
			String cols[] = line.split("\\|");
			LeafValue ret[] = new LeafValue[cols.length];
			
			for(int i=0; i<cols.length; i++) {
				
				String type = schema.getColumns().get(i).getColumnType();
				
				switch(type) {
				case "int":
					ret[i] = new LongValue(cols[i]);
					break;
				
				case "decimal":
					ret[i] = new DoubleValue(cols[i]);
					break;
				
				case "char":
				case "varchar":
				case "string":
					// Blank spaces are appended to account for JSQLParser's weirdness
					ret[i] = new StringValue(" "+cols[i]+" ");
					break;

				case "date":
					// Blank spaces are appended to account for JSQLParser's weirdness
					ret[i] = new DateValue(" "+cols[i]+" ");
					break;
				
				default:
					throw new SQLException();
				}
			}
			
			return ret;
			
		} catch (IOException e) {
			System.err.println("IOException on Scan Operator");
		} catch (SQLException e) {
			System.err.println("SQLException on Scan Operator");
		}
		return null;
	}

	@Override
	public void reset() {
		try {
			br = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			System.err.println("File "+ f + " not found");
		}
	}
	
	

	@Override
	public Schema getSchema() {
		return schema;
	}

}
