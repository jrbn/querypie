package nl.vu.cs.querypie.reasoning.expand;

public abstract class Node {
	private final int id;
	protected final Tree tree;

	public Node parent = null;
	public Node sibling = null;
	public Node child = null;
	public int height = 0;

	public Node(final int id, final Tree tree) {
		this.id = id;
		this.tree = tree;
	}

	public final int getId() {
		return id;
	}
}