package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class QueryPreprocessor {
	
	
	/* Build the indexes for all the schemas */
	public static void buildIndex(Schema s) {
		
		/* Initializations */
		Environment db = null;
		Database table = null;
		
		try {
			
			/* File read initializations */
			BufferedReader br = new BufferedReader(new FileReader(new File(s.getTableFile())));
			String line = null;
			
			/* Environment Configuration */
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setLocking(false);

			/* Database Configuration */
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);

			db = new Environment(Main.indexDirectory, envConfig);
			table = db.openDatabase(null, s.getTableName(), dbConfig);

			
			while( (line = br.readLine()) != null ) {
				
				gatherStatistics(line, s);
				
				ByteArrayOutputStream keyOut = getKeyByteArrayFromLine(line, s);
				ByteArrayOutputStream valOut = getValByteArrayFromLine(line, s);
				
				DatabaseEntry key = new DatabaseEntry(keyOut.toByteArray());
				DatabaseEntry val = new DatabaseEntry(valOut.toByteArray());
				
				table.put(null, key, val);
			}
			
			br.close();
			
		}
		catch (DatabaseException | IOException e) {
			e.printStackTrace();
		}
		finally {
			/* Close everything */
			if(table != null) {
				table.close();
			}
			if(db != null) {
				db.close();
			}
			
			/* DEBUG */
			if(Main.DEBUG) {
				System.err.println("Primary index buit for "+s.getTableName());
			}
		}
	}
	
	




	private static void gatherStatistics(String line, Schema s) {
		s.incrementRowCount();
	}
	
	
	
	
	/*
	 * Helpers
	 */
	public static ByteArrayOutputStream getKeyByteArrayFromLine(String line,
			Schema s) throws NumberFormatException, IOException {
		
		String[] tuple = line.split("\\|");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		
		for(int i=0; i<tuple.length; i++) {
			if(s.getPrimaryKey().contains(s.getColumns().get(i))) {
				writeToDataOutputStream(dos, tuple, i, s);
			}
		}
		
		return out;
	}


	public static ByteArrayOutputStream getValByteArrayFromLine(
			String line, Schema s) throws NumberFormatException, IOException {
		
		String[] tuple = line.split("\\|");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		
		for(int i=0; i<tuple.length; i++) {
			writeToDataOutputStream(dos, tuple, i, s);
		}
		
		return out;
	}
	
	

	
	private static void writeToDataOutputStream(DataOutputStream dos,
			String[] tuple, int i, Schema s) throws NumberFormatException, IOException {
		
		switch(s.getColumns().get(i).getColumnType()) {
		case "int":
			dos.writeLong(Integer.parseInt(tuple[i]));
			break;
		
		case "decimal":
			dos.writeDouble(Double.parseDouble(tuple[i]));
			break;
		
		case "char":
		case "varchar":
		case "string":
		case "date":
			dos.writeUTF(" "+tuple[i]+" ");
			break;
		}
		
	}






	public static Schema generateSecondaryIndexes(Schema s) {
		String name = s.getTableName();
		ArrayList<ColumnWithType> columns = s.getColumns();
		
		switch(name) {
		case "LINEITEM":
			s.addToSecondaryIndexes(columns.get(0));
			s.addToSecondaryIndexes(columns.get(8));
			s.addToSecondaryIndexes(columns.get(10));
			break;
		case "ORDERS":
			s.addToSecondaryIndexes(columns.get(4));
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
