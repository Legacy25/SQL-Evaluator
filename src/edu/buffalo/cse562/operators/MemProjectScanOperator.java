package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;

import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class MemProjectScanOperator implements Operator {

	private Schema oldSchema;
	private Schema newSchema;
	
	
	private HashSet<String> selectedColumnNames;
	private boolean[] selectedCols;
	private int index = 0;
	
	public MemProjectScanOperator(Schema schema, HashSet<String> selectedColumnNames) {
		this.oldSchema = schema;
		this.selectedColumnNames = selectedColumnNames;
		
		
		selectedCols = new boolean[schema.getColumns().size()];
		Arrays.fill(selectedCols, false);
		
		buildSchema();
	}
	
	private void buildSchema() {
		newSchema = new Schema(oldSchema);
		newSchema.clearColumns();
		
		int i = 0;
		for(ColumnWithType c : oldSchema.getColumns()) {
			if(selectedColumnNames.contains(c.getColumnName().toLowerCase())) {
				newSchema.addColumn(c);
				selectedCols[i] = true;
			}
			
			i++;
		}
	}
	
	@Override
	public Schema getSchema() {
		return newSchema;
	}

	@Override
	public void generateSchemaName() {
		
	}

	@Override
	public void initialize() {
		buildSchema();
		index = 0;
	}

	@Override
	public LeafValue[] readOneTuple() {
		LeafValue[] line = null;
		try {
			line = oldSchema.getTuple(index);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		index++;
		
		if(line == null)
			return null;
		
		return constructTuple(line);
	}

	public LeafValue[] constructTuple(LeafValue[] line) {
		
		/* LeafValue array that will hold the tuple to be returned */
		LeafValue ret[] = new LeafValue[newSchema.getColumns().size()];
		
		/* 
		 * Iterate over each column according to schema's type information
		 * and populate the ret array with appropriate value and LeafValue
		 * type
		 */
		int k = 0;
		for(int i=0; i<line.length; i++) {
			if(selectedCols[i]) {
				ret[k] = line[i];
				k++;
			}
		}
		
		/* Return the generated tuple */
		return ret;
	}
	
	@Override
	public void reset() {
		index = 0;
	}

	@Override
	public Operator getLeft() {
		return null;
	}

	@Override
	public Operator getRight() {
		return null;
	}

	@Override
	public void setLeft(Operator o) {
		
	}

	@Override
	public void setRight(Operator o) {
		
	}

	public Schema getOldSchema() {
		return oldSchema;
	}

	public HashSet<String> getSelectedColumns() {
		return selectedColumnNames;
	}
}
