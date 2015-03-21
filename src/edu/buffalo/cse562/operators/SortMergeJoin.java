package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.schema.ColumnWithType;
import edu.buffalo.cse562.schema.Schema;

public class SortMergeJoin implements Operator {

	private Expression where;
	
	private Operator child1, child2;
	
	
	private Schema schema;
	
	public SortMergeJoin(Expression where, Operator child1, Operator child2) {
		this.where = where;
		this.child1 = child1;
		this.child2 = child2;
		
		buildSchema();
	}
	
	public SortMergeJoin(GraceHashJoinOperator ghjOp) {
		this.where = ghjOp.getWhere();
		this.child1 = ghjOp.getLeft();
		this.child2 = ghjOp.getRight();
		
		buildSchema();
	}

	private void buildSchema() {
		
		schema = new Schema();
		generateSchemaName();
		
		for(ColumnWithType c:child1.getSchema().getColumns()) {
			schema.addColumn(c);
		}
		
		for(ColumnWithType c:child2.getSchema().getColumns()) {
			schema.addColumn(c);
		}
		
	}
	
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void generateSchemaName() {
		child1.generateSchemaName();
		child2.generateSchemaName();
		
		schema.setTableName(
				child1.getSchema().getTableName() +
				" \u2A1D" +
				" {" + where + "} " +
				child2.getSchema().getTableName()
				);
	}

	@Override
	public void initialize() {
		child1.initialize();
		child2.initialize();

		buildSchema();
	}

	@Override
	public LeafValue[] readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		child1.reset();
		child2.reset();
		
	}

	@Override
	public Operator getLeft() {
		return child1;
	}

	@Override
	public Operator getRight() {
		return child2;
	}

	@Override
	public void setLeft(Operator o) {
		child1 = o;
	}

	@Override
	public void setRight(Operator o) {
		child2 = o;
	}

}
