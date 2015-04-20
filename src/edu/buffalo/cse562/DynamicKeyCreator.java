package edu.buffalo.cse562;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.schema.Column;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class DynamicKeyCreator implements SecondaryKeyCreator {

	private Schema schema;
	private Column column;
	
	public DynamicKeyCreator(Schema s, Column col) {
		this.schema = s;
		this.column = col;
	}
	
	@Override
	public boolean createSecondaryKey(SecondaryDatabase arg0,
			DatabaseEntry arg1, DatabaseEntry arg2, DatabaseEntry arg3) {

		try {
			ByteArrayInputStream in = new ByteArrayInputStream(arg2.getData());
			DataInputStream dis = new DataInputStream(in);
			ArrayList<ColumnWithType> colList = schema.getColumns();
			
			Long lValue = null;
			String sValue = null;
			Double dValue = null;
			
			for(int i=0; i<colList.size(); i++) {
				String type = colList.get(i).getColumnType();
				
				switch(type) {
				case "int":
					lValue = dis.readLong();
					break;
				
				case "decimal":
					dValue = dis.readDouble();
					break;
				
				case "char":
				case "varchar":
				case "string":
				case "date":
					sValue = dis.readUTF();
					break;
				}
				
				if(colList.get(i).getColumnName().equalsIgnoreCase(column.getColumnName())) {
					ByteArrayOutputStream out = new ByteArrayOutputStream();
					DataOutputStream dos = new DataOutputStream(out);
					
					switch(type) {
					case "int":
						dos.writeLong(lValue);
						break;
					
					case "decimal":
						dos.writeDouble(dValue);
						break;
					
					case "char":
					case "varchar":
					case "string":
					case "date":
						dos.writeUTF(sValue.substring(1, sValue.length() - 1));
						break;
					}
					
					arg3.setData(out.toByteArray());
					return true;
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}

}
