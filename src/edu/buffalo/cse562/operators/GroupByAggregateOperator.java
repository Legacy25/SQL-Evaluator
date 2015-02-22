package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class GroupByAggregateOperator implements Operator {

	private Schema schema;
	private Operator child;
	private HashMap<String, Boolean> seenValues;
	private ArrayList<Column> columns;
	private List<SelectItem> selectItems;
	private boolean[] selectedCols;
	
	public GroupByAggregateOperator(ArrayList<Column> columns, List<SelectItem> selectItems, Operator child) {
	
		this.child = child;
		this.columns = columns;
		this.selectItems = selectItems;
		
		schema = child.getSchema();
		
		selectedCols = new boolean[schema.getColumns().size()];
		Arrays.fill(selectedCols, false);
		getSelectedColumns();
		
		seenValues = new HashMap<String, Boolean>();
	}
	
	private void getSelectedColumns() {
		
		ArrayList<ColumnWithType> schemaCols = schema.getColumns();
		
		for(int i=0; i<columns.size(); i++) {
			for(int j=0; j<schemaCols.size(); j++) {
				if(columns.get(i).getColumnName().equalsIgnoreCase(schemaCols.get(j).getColumnName())
						|| columns.get(i).getWholeColumnName().equalsIgnoreCase(schemaCols.get(j).getWholeColumnName())) {
					selectedCols[j] = true;
				}
			}
		}
		
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public LeafValue[] readOneTuple() {

		LeafValue[] next = child.readOneTuple();
		if(next == null) {
			return null;
		}
		
		String key = "";
		
		for(int i=0; i<schema.getColumns().size(); i++) {
			if(selectedCols[i]) {
				key += next[i].toString();
			}
		}
		
		if(seenValues.containsKey(key)) {
			return readOneTuple();
		}
		else {
			seenValues.put(key, true);
			return next;
		}
		
	}

	@Override
	public void reset() {
		child.reset();
		seenValues.clear();
	}

}
