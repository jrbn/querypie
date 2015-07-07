package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class Tree {
	private ArrayList<QueryNode> queries = new ArrayList<QueryNode>();
	private ArrayList<RuleNode> rules = new ArrayList<RuleNode>();
	private ArrayList<QueryNode> computedQueries = new ArrayList<QueryNode>();

	public QueryNode newRoot() {
		QueryNode q = new QueryNode(null, queries.size(), this);
		queries.add(q);
		return q;
	}

	public synchronized QueryNode newQuery(Node parent) {
		QueryNode q = new QueryNode(parent, queries.size(), this);
		queries.add(q);
		return q;
	}

	public synchronized RuleNode newRule(Node parent, Rule rule) {
		RuleNode q = new RuleNode(parent, rule, rules.size(), this);
		rules.add(q);
		return q;
	}

	public QueryNode getQuery(int id) {
		return queries.get(id);
	}

	public RuleNode getRule(int id) {
		return rules.get(id);
	}

	public int getNQueries() {
		return queries.size();
	}

	public boolean isComputed(final QueryNode query, final ActionContext context) {
		QueryNode[] n;
		synchronized (this) {
			n = computedQueries.toArray(new QueryNode[computedQueries.size()]);
		}
		for (QueryNode completedQuery : n) {
			if (query.isContainedIn(completedQuery, context)) {
				return true;
			}
		}
		return false;
	}

	public synchronized void addToComputed(QueryNode query) {
		computedQueries.add(query);
	}
}
