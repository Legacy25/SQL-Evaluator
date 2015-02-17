package edu.buffalo.cse562.operators;

public class JoinOperator implements Operator {

	Operator child1, child2;
	Long next1[];
	Long next2[];
	boolean flag;

	public JoinOperator(Operator child1, Operator child2) {
		this.child1 = child1;
		this.child2 = child2;
		next1 = null;
		next2 = null;
		flag = true;
	}
	
	@Override
	public Long[] readOneTuple() {
		
		if(flag) {
			flag = false;
			next1 = child1.readOneTuple();
			if(next1 == null)
				return null;
		}

		next2 = child2.readOneTuple();
		if(next2 == null) {
			flag = true;
			child2.reset();
			return readOneTuple();
		}
		
		int length = next1.length + next2.length;
		
		Long ret[] = new Long[length];
		for(int i=0; i<length; i++) {
			if(i<next1.length)
				ret[i] = next1[i];
			else
				ret[i] = next2[i-next1.length];
		}
		
		return ret;
	}

	@Override
	public void reset() {
		child1.reset();
		child2.reset();
		flag = true;
	}

}
