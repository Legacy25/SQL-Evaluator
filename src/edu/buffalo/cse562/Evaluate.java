package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.HashMap;

import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class Evaluate extends Eval {

	private HashMap<String, Schema> tables;
	
	public Evaluate() {
		tables = ParseTreeGenerator.getTableSchemas();
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		Schema schema = tables.get(arg0.getTable().getName().toString());
		String type = null;
		LeafValue lv = null;
		String value = "0";
		
		for(int i=0; i<schema.getColumns().size(); i++) {
			if(schema.getColumns().get(i).getColumnName().equals(arg0.getColumnName())) {
				type = schema.getColumns().get(i).getColumnType();
				break;
			}
		}
		
		if(type == null) {
			System.err.println("Invalid type");
			System.exit(0);
		}
		switch(type) {
		case "int":
			lv = new LongValue(value);
			break;
		case "decimal":
			lv = new DoubleValue(value);
			break;
		case "varchar":
			
		case "char":
			
		case "string":
			lv = new StringValue(value);
			break;
		case "date":
			lv = new DateValue(value);
			break;
		}
					
		return lv;
	}

}
