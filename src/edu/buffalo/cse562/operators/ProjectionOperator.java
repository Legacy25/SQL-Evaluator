package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator implements Operator {

	private Schema schema, childSchema;
	private List<SelectItem> selectItems;
	private Operator child;
	private boolean selectAll;
	private boolean columnsActive[];
	private int columnsBefore, columnsAfter;
	
	public ProjectionOperator(List<SelectItem> selectItems, Operator child) {
	
		childSchema = child.getSchema();
		schema = new Schema("PROJECT [" + childSchema.getTableName() + "]", "__mem__");
		
		columnsBefore = childSchema.getColumns().size();
		
		this.selectItems = selectItems;
		this.child = child;
		
		selectAll = false;
		columnsActive = new boolean[columnsBefore];
		
		for(int i=0; i<columnsBefore; i++) {
			columnsActive[i] = false;
		}
		
		columnsAfter = 0;
		generateProjection();
	}
	
	private void generateProjection() {
		
		if(selectItems.size() == 1 && selectItems.get(0).toString().equals("*")) {
			selectAll = true;
			return;
		}
		
		columnsAfter = selectItems.size();
		ArrayList<String> columnsNames = new ArrayList<String>();
		for(int i=0; i<columnsAfter; i++) {
			columnsNames.add(selectItems.get(i).toString());
		}
		
		for(int i=0; i<columnsBefore; i++) {
			if(columnsNames.contains(childSchema.getColumns().get(i).getColumnName())
					|| columnsNames.contains(childSchema.getColumns().get(i).getTable().toString()
							+ "." + childSchema.getColumns().get(i).getColumnName())) {
				columnsActive[i] = true;
				schema.addColumn(childSchema.getColumns().get(i));
			}
		}
		
	}

	@Override
	public Long[] readOneTuple() {
		Long next[] = child.readOneTuple();
		if(next == null)
			return null;
		
		if(selectAll)
			return next;
		
		Long ret[] = new Long[columnsAfter];
		
		int k=0;
		for(int i=0; i<columnsBefore; i++) {
			if(columnsActive[i]) {
				ret[k] = next[i];
				k++;
			}
		}
		return ret;
	}

	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

}
