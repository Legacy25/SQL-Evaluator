package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class ScanOperator implements Operator {

	BufferedReader br;
	File f;
	
	public ScanOperator(String file) {
		f = new File(file);
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

}
