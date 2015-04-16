package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.buffalo.cse562.schema.Schema;

public class QueryPreprocessor {
	
	
	/* Build the indexes for all the schemas */
	public static void buildIndex(Schema s) {
		
		Environment db = null;
		Database table = null;
		
		try {
			
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			db = new Environment(Main.indexDirectory, envConfig);
			DatabaseConfig dbCon = new DatabaseConfig();
			dbCon.setAllowCreate(true);
			table = db.openDatabase(null, s.getTableName(), dbCon);
			
			BufferedReader br = new BufferedReader(new FileReader(new File(s.getTableFile())));
			String[] tuple = null;
			String line = null;

			Integer uid = 0;
			
			while( (line = br.readLine()) != null ) {
				tuple = line.split("\\|");
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out);
				
				for(int i=0; i<tuple.length; i++) {
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
				DatabaseEntry key = null;
				if(s.getTableName().equalsIgnoreCase("LINEITEM")) {
					key = new DatabaseEntry(uid.toString().getBytes());
					uid++;
				}
				else {
					key = new DatabaseEntry(tuple[0].toString().getBytes());
				}
				DatabaseEntry val = new DatabaseEntry(out.toByteArray());
				
				table.put(null, key, val);
			}
			br.close();
			
		} catch (DatabaseException | IOException e) {
			e.printStackTrace();
		} finally {
			if(table != null) {
				table.close();
			}
			if(db != null) {
				db.close();
			}
		}
	}

	

	/*
	 * Static methods for pre-processing data
	 * Indexing and cost-based optimizations happen here
	 */
}
