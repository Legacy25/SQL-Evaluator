package edu.buffalo.cse562.operators;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.schema.Schema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import edu.buffalo.cse562.LeafValueComparator;
import edu.buffalo.cse562.Main;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

public class ExternalSortOperator implements Operator {

	/*
	 * External Sort Operator
	 * 		Sorts the child relation without blocking
	 * 
	 * Constructor Variables
	 * 		Order By attributes list
	 * 		The child operator
	 * 
	 * Working Set Size - Memory buffer size
	 */
	private Schema schema;				/* Schema for this table */

	private Operator child;				/* The child operator to be sorted */


	/* Holds the sort key attributes */
	private ArrayList<OrderByElement> arguments;
	
	/* Holds the sorted relation in memory temporarily */
	private ArrayList<LeafValue[]> tempList;

	/* For book-keeping	 */
	private HashMap<Integer, String> tempFileMap;
	private HashMap<Integer, String> finalFileMap;
	private ArrayList<BufferedReader> filePointers;

	
	/* Maximum number of rows at a time in memory, i.e. maximum rows in a swap file*/
	private int maxRows;

	LeafValueComparator lvc;
	BufferedReader br;

	
	
	public ExternalSortOperator(OrderByOperator o, int maxRows) {
		this.arguments = o.getArguments();
		this.child = o.getLeft();
		this.maxRows = maxRows;

		schema = new Schema(child.getSchema());

		/* Set an appropriate table name, for book-keeping */
		generateSchemaName();
		

		tempList = new ArrayList<LeafValue[]>();
		tempFileMap = new HashMap<Integer, String>();
		finalFileMap = new HashMap<Integer, String>();
		filePointers = new ArrayList<BufferedReader>();

		br = null;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void generateSchemaName() {
		child.generateSchemaName();
		schema.setTableName("O{ext} (" + child.getSchema().getTableName() + ")");
	}

	@Override
	public void initialize() {
		child.initialize();
		schema = new Schema(child.getSchema());

		/* Set an appropriate table name, for book-keeping */
		generateSchemaName();
		
		lvc = new LeafValueComparator(arguments, schema);
		
		partitionAndSortData();
		
		System.gc();

		try {
			mergeSortedPartitions();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.gc();
	}

	private void partitionAndSortData() {
		
		String fileName = "", tuple = "";
		int j = 0, counter = 0;

		LeafValue[] next = child.readOneTuple();
		
		while (next != null) {
			while ((j < maxRows) && (next != null)) {
				tempList.add(next);
				next = child.readOneTuple();
				j++;
			}

			if (j == maxRows || next == null) {
				/* Sort tempList using the sorting routine */
				Collections.sort(tempList, new LeafValueComparator(arguments, schema));
				
				fileName = Integer.valueOf(Main.fileUUID++).toString()
						+ "_block_"
						+ counter
						+ ".txt";
				
				BufferedWriter bw = null;
				
				try {
					File f = new File(Main.swapDirectory, fileName);
					bw = new BufferedWriter(new FileWriter(f));
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				Iterator<LeafValue[]> i = tempList.iterator();
				while (i.hasNext()) {
					LeafValue[] lv = i.next();
					tuple = serializeTuple(lv) + (i.hasNext() ? "\n" : "");
					try {
						bw.write(tuple);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				tempList.clear();
				j = 0;
				tempFileMap.put(counter, fileName);
				
				counter++;
				
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		tempList.clear();
		
		/* 
		 * Reset the child since we read it
		 * This preserves the semantics of the Operator interface
		 */
		child.reset();
	}

	private String serializeTuple(LeafValue[] leafvalue) {
		
		String row = "";
		
		for (int i=0; i<leafvalue.length; i++) {
			String temp = leafvalue[i].toString();
			if(temp.startsWith("\'") && temp.endsWith("\'")) {
				temp = temp.substring(1, temp.length() - 1);
			}
			row += temp + "|";
		}
		
		return row.substring(0, row.length() - 1);
	}

	public LeafValue[] unSerializeTuple(String row) {
		
		String tokens[] = row.split("\\|");
		String type = "";

		LeafValue[] tuple = new LeafValue[tokens.length];
		
		for (int i=0; i<tokens.length; i++) {
			type = schema.getColumns().get(i).getColumnType();
			
			switch(type) {
			case "int":
				tuple[i] = new LongValue(tokens[i]);
				break;

			
			case "decimal":
				tuple[i] = new DoubleValue(tokens[i]);
				break;

			case "char":
			case "varchar":
			case "string":
				/* Blank spaces are appended to account for JSQLParser's weirdness */
				tuple[i] = new StringValue(" "+tokens[i]+" ");
				break;

			case "date":
				/* Same deal as string */
				tuple[i] = new DateValue(" "+tokens[i]+" ");
				break;
			default:
				System.err.println("Unknown column type");
			}

		}
		return tuple;
	}

	public void mergeSortedPartitions() throws IOException {
		
		BufferedReader br1 = null, br2 = null;
		int fileMapCounter = 0, round = 0;
		HashMap<Integer, String> tempFileMap1 = new HashMap<Integer, String>();
		String filePrefix = "";
		
		while (tempFileMap.size() > 1) {

			openFilePointers();
			for (int i=0;i<tempFileMap.size();i+=2) {
				br1 = filePointers.get(i);
				filePrefix += i;

				if (((i+1) < tempFileMap.size()) && (filePointers.get(i+1) != null)) {
					br2 = filePointers.get(i+1);
					filePrefix += i+1;
				}

				merge(br1,br2, tempFileMap1, filePrefix,round, fileMapCounter++);
				filePrefix = "";

			}

			tempFileMap.clear();
			fileMapCounter = 0;
			
			for (int k=0;k<tempFileMap1.size();k++) {
				tempFileMap.put(k, tempFileMap1.get(k));
			}
			
			tempFileMap1.clear();
			round++;
			
			closeFilePointers();
		}
		
		finalFileMap = tempFileMap;
		
	}

	public void openFilePointers() {

		for (int i=0; i<tempFileMap.size(); i++) {
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(Main.swapDirectory, tempFileMap.get(i)))));
				filePointers.add(br);
				br = null;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public void closeFilePointers() {
		
		for (int i=0; i<filePointers.size(); i++) {
			try {
				filePointers.get(i).close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		filePointers.clear();
	}

	public void merge(BufferedReader br1, BufferedReader br2, HashMap<Integer, String> tempFileMap_1, String filePrefix, int round, int fileMapCounter2) throws IOException {
		
		ArrayList<LeafValue[]> tempBuffer = new ArrayList<LeafValue[]>();
		int fileMapCounter = 0, result = 0;
		
		String temp1, temp2 = null;
		BufferedWriter bw = null;
		
		String fileName = "/"+ Main.fileUUID +"_part_" + filePrefix + "_" + round + ".txt";
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(Main.swapDirectory, fileName))));
		temp1 = br1.readLine();

		if (br2 == null) {
			while (temp1 != null) {
				tempBuffer.add(unSerializeTuple(temp1));
				temp1 = br1.readLine();
				if (tempBuffer.size() == maxRows || temp1 == null) {
					flushToDisk(tempBuffer, fileMapCounter++, bw);
					tempBuffer.clear();
				}
			}
			return;
		} 
		
		if (br2 != null) {
			temp2 = br2.readLine();
		}
		
		while (!(temp1 == null && temp2 == null)) {
			if (temp1 == null) {
				while (temp2 != null) {
					tempBuffer.add(unSerializeTuple(temp2));
					temp2 = br2.readLine();
					if (tempBuffer.size() == maxRows || temp2 == null) {
						flushToDisk(tempBuffer, fileMapCounter++, bw);
						tempBuffer.clear();
					}
				}
			}
			
			if (temp2 == null) {
				while (temp1 != null) {
					tempBuffer.add(unSerializeTuple(temp1));
					temp1 = br1.readLine();
					if (tempBuffer.size() == maxRows || temp1 == null) {
						flushToDisk(tempBuffer, fileMapCounter++, bw);
						tempBuffer.clear();
					}
				}
			}
			
			if (temp1 != null && temp2 != null) {
				result = compareTuples(temp1, temp2);
				
				if (result <= 0) {
					tempBuffer.add(unSerializeTuple(temp1));
					temp1 = br1.readLine();
				}
				else if (result > 0) {
					tempBuffer.add(unSerializeTuple(temp2));
					temp2 = br2.readLine();
				}
				
				if (tempBuffer.size() == maxRows || temp1 == null || temp2 == null) {
					flushToDisk(tempBuffer, fileMapCounter++, bw);
					tempBuffer.clear();
				}
			}
			
		}


		bw.close();
		tempFileMap_1.put(fileMapCounter2, fileName);
	}

	public int compareTuples(String temp1, String temp2) {
		
		LeafValue[] o1 = unSerializeTuple(temp1);
		LeafValue[] o2 = unSerializeTuple(temp2);
		
		return lvc.compare(o1, o2);
		
	}

	private void flushToDisk(ArrayList<LeafValue[]> tempBuffer, int fileMapCounter, BufferedWriter bw) throws IOException {

		String tuple = "";
		
		Iterator<LeafValue[]> i = tempBuffer.iterator();
		while (i.hasNext()) {
			LeafValue[] lv = i.next();
			tuple = serializeTuple(lv) + "\n";
			bw.write(tuple);
		}
	}
	
	@Override
	public LeafValue[] readOneTuple() {
		String fileName = "";
		String row = "";
		
		if (br == null) {
			fileName = finalFileMap.get(0);
			try {
				br = new BufferedReader(new FileReader(new File(Main.swapDirectory, fileName)));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		try {
			if ((row = br.readLine()) != null) {
				return unSerializeTuple(row);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;


	}

	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public Operator getLeft() {
		return child;
	}

	@Override
	public Operator getRight() {
		return null;
	}

	@Override
	public void setLeft(Operator o) {
		this.child = o;
	}

	@Override
	public void setRight(Operator o) {
		/* Nothing to do */
	}

}
