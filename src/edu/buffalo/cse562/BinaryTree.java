package edu.buffalo.cse562;

public class BinaryTree<T> {
	
	private T node;
	private BinaryTree<T> left, right;
	
	public enum Side {
		LEFT, RIGHT;
	}
	
	public BinaryTree() {
		node = null;
		left = null;
		right = null;
	}
	
	public BinaryTree(T root) {
		node = root;
		left = null;
		right = null;
	}
	
	public T getRoot() {
		return node;
	}
	
	public void setRoot(T root) {
		node = root;
	}
	
	public void insertRoot(T root) {
		this.left = this;
		this.right = null;
		this.node = root;
	}
	
	public void insertBranch(BinaryTree<T> branch, Side s) throws NonEmptyBranchException {
		switch (s) {
		case LEFT:
			if(left == null) {
				left = branch;
			}
			else {
				throw new NonEmptyBranchException();
			}
			break;

		case RIGHT:
			if(right == null) {
				right = branch;
			}
			else {
				throw new NonEmptyBranchException();
			}
			break;
			
		default:
			break;
		}
	}
	
}
