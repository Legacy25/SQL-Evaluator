package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.schema.Schema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends Eval implements Operator {

	private Schema schema, childSchema;
	private List<SelectItem> selectItems;
	private Operator child;
	private boolean selectAll;
	private boolean columnsActive[];
	private int columnsBefore, columnsAfter;
	private LeafValue next[];
	
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
					|| columnsNames.contains(childSchema.getColumns().get(i).getWholeColumnName())) {
				columnsActive[i] = true;
				schema.addColumn(childSchema.getColumns().get(i));
			}
		}
		
	}

	@Override
	public LeafValue[] readOneTuple() {
		next = child.readOneTuple();
		if(next == null)
			return null;
		
		if(selectAll)
			return next;
		
		LeafValue ret[] = new LeafValue[columnsAfter];
		
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

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		LeafValue lv = null;
		int i;
		String type = null;
		
		for(i=0; i<schema.getColumns().size(); i++) {
			if(arg0.getColumnName().equals(schema.getColumns().get(i).toString())
					|| arg0.getWholeColumnName().equals(schema.getColumns().get(i).toString())) {
				type = schema.getColumns().get(i).getColumnType();
				break;
			}
		}
		
		switch(type) {
		case "int":
			try {
				lv = new LongValue(next[i].toLong());
			} catch (InvalidLeaf e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "decimal":
			try {
				lv = new DoubleValue(next[i].toDouble());
			} catch (InvalidLeaf e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "char":
		case "varchar":
		case "string":
			lv = new StringValue(next[i].toString());
			break;
		case "date":
			lv = new DateValue(next[i].toString());
			break;
		default:
			throw new SQLException();
		}
		
		return lv;
	}


}
