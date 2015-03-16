package edu.buffalo.cse562.datastructures;

import edu.buffalo.cse562.exceptions.InsertOnNonEmptyBranchException;

public class ParseTree<T> {
	
	private T node;
	private ParseTree<T> left, right;
	private int levels;

	public enum Side {
		LEFT, RIGHT;
	}
	
	public ParseTree() {
		node = null;
		left = null;
		right = null;
		
		levels = 0;
	}
	
	public ParseTree(T root) {
		node = root;
		left = null;
		right = null;
		
		levels = 1;
	}
	
	public T getRoot() {
		return node;
	}
	
	public void setRoot(T root) {
		node = root;
	}
	
	public ParseTree<T> getLeft() {
		return left;
	}

	public void setLeft(ParseTree<T> left) {
		this.left = left;
	}

	public ParseTree<T> getRight() {
		return right;
	}

	public void setRight(ParseTree<T> right) {
		this.right = right;
	}
	
	public void insertRoot(T root) {
		this.left = this;
		this.right = null;
		this.node = root;
		
		levels++;
	}
	
	public void insertBranch(ParseTree<T> branch, Side s) throws InsertOnNonEmptyBranchException {
		switch (s) {
		case LEFT:
			if(left == null) {
				left = branch;
			}
			else {
				throw new InsertOnNonEmptyBranchException();
			}
			break;

		case RIGHT:
			if(right == null) {
				right = branch;
			}
			else {
				throw new InsertOnNonEmptyBranchException();
			}
			break;
			
		default:
			break;
		}
	}
}
