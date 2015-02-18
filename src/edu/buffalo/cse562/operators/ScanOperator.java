package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import edu.buffalo.cse562.schema.Schema;

public class ScanOperator implements Operator {

	Schema schema;
	BufferedReader br;
	File f;
	
	
	public ScanOperator(Schema schema) {
		this.schema = schema;
		f = new File(schema.getTableFile());
		br = null;
		reset();
	}
	
	@Override
	public Long[] readOneTuple() {
		if(br == null) {
			return null;
		}

		String line = null;
		try {
			if((line = br.readLine()) == null) {
				return null;
			}
			String cols[] = line.split("\\|");
			Long ret[] = new Long[cols.length];
			
			for(int i=0; i<cols.length; i++) {
				ret[i] = Long.decode(cols[i]);
			}
			
			return ret;
			
		} catch (IOException e) {
			System.err.println("IOException on Scan Operator");
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
