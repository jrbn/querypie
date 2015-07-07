package nl.vu.cs.querypie.reasoning.expand;

import java.util.LinkedList;

public class Queue {

	LinkedList<QueryNode> queries = new LinkedList<QueryNode>();

	public synchronized void addNode(QueryNode node) {
		queries.addFirst(node);
	}

	public QueryNode get() {
		return queries.removeFirst();
	}

	public int size() {
		return queries.size();
	}

	public void clear() {
		queries.clear();
	}
}
