package edu.buffalo.cse562;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

public class Evaluate extends Eval {

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		LeafValue lv = new LeafValue() {
			
			@Override
			public long toLong() throws InvalidLeaf {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public double toDouble() throws InvalidLeaf {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		
		return lv;
	}

}
