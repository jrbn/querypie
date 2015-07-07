package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class Tree {

	// private final static class ListInfo {
	// TupleSet set;
	// QueryNode head, query;
	// RuleNode rule;
	// }

	private final ArrayList<QueryNode> queries = new ArrayList<QueryNode>();
	private final ArrayList<RuleNode> rules = new ArrayList<RuleNode>();
	private final ArrayList<QueryNode> computedQueries = new ArrayList<QueryNode>();
	private final ArrayList<RuleNode> removedRules = new ArrayList<RuleNode>();

	// private final ArrayList<ListInfo> cacheList = new ArrayList<ListInfo>();

	public QueryNode newRoot() {
		final QueryNode q = new QueryNode(null, queries.size(), this);
		queries.add(q);
		return q;
	}

	public synchronized QueryNode newQuery(Node parent) {
		final QueryNode q = new QueryNode(parent, queries.size(), this);
		queries.add(q);
		return q;
	}

	public synchronized RuleNode newRule(Node parent, Rule rule) {
		final RuleNode q = new RuleNode(parent, rule, rules.size(), this);
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
		for (final QueryNode completedQuery : n) {
			if (query.isContainedIn(completedQuery, context)) {
				return true;
			}
		}
		return false;
	}

	public int getNUpdatedQueries() {
		int n = 0;
		for (QueryNode query : queries) {
			if (query != null) {
				if (query.isUpdated())
					n++;
			}
		}
		return n;
	}

	public synchronized void addToComputed(QueryNode query) {
		computedQueries.add(query);
	}

	public String info() {
		String line = "";

		// Stats rules:
		int rule1 = 0;
		int removedRule1 = 0;
		int filteredRule1 = 0;
		int rule2 = 0;
		int removedRule2 = 0;
		int filteredRule2 = 0;
		int rule3 = 0;
		int removedRule3 = 0;
		int filteredRule3 = 0;
		int rule4 = 0;
		int removedRule4 = 0;
		int filteredRule4 = 0;

		for (final RuleNode r : rules) {
			if (r.rule.type == 1) {
				rule1++;
				if (r.idFilterValues != -1) {
					filteredRule1++;
				}
			} else if (r.rule.type == 2) {
				rule2++;
				if (r.idFilterValues != -1) {
					filteredRule2++;
				}
			} else if (r.rule.type == 3) {
				rule3++;
				if (r.idFilterValues != -1) {
					filteredRule3++;
				}
			} else {
				rule4++;
				if (r.idFilterValues != -1) {
					filteredRule4++;
				}
			}
		}

		for (final RuleNode r : removedRules) {
			if (r.rule.type == 1) {
				removedRule1++;
			} else if (r.rule.type == 2) {
				removedRule2++;
			} else if (r.rule.type == 3) {
				removedRule3++;
			} else {
				removedRule4++;
			}
		}

		line = "Rules1 " + rule1 + "(" + removedRule1 + ") Rules2 " + rule2
				+ "(" + removedRule2 + ") Rules3 " + rule3 + "(" + removedRule3
				+ ") Rules4 " + rule4 + "(" + removedRule4 + ")\n";
		line += "Filtered Rules1 " + filteredRule1 + " Filtered Rules2 "
				+ filteredRule2 + " Filtered Rules3 " + filteredRule3
				+ " Filtered Rules4 " + filteredRule4 + "\n";
		line += "N. queries " + queries.size() + " computed "
				+ computedQueries.size() + " removed "/* + removedRules.size() */
				+ "\n";

		// Search max height of the tree
		int maxHeight = 0;
		int nExpanded = 0;
		int isNew = 0;
		int isNewChecked = 0;
		for (final QueryNode node : queries) {
			if (node != null) {
				if (node.height > maxHeight) {
					maxHeight = node.height;
				}
				if (node.isExpanded()) {
					nExpanded++;
				}
				if (node.isNew()) {
					isNew++;
				}
				if (node.checkedNew()) {
					isNewChecked++;
				}
			}
		}
		line += "Max height " + maxHeight + " Expanded " + nExpanded
				+ " isNew " + isNew + " CheckedIsNew " + isNewChecked + "\n";

		return line;
	}

	public synchronized QueryNode removeLastQuery() {
		return queries.remove(queries.size() - 1);
	}

	public synchronized void removeRule(RuleNode output) {
		removedRules.add(output);
	}

	public synchronized boolean isNew(QueryNode queryNode, ActionContext context) {
		for (final QueryNode q : queries) {
			if (q != null) {
				if (queryNode.getId() > q.getId()
						&& queryNode.isContainedIn(q, context)) {
					return false;
				}
			}
		}
		return true;
	}

	public String printAllQueries() {
		String output = "";
		for (final QueryNode query : queries) {
			if (query != null) {
				output += query.getS() + " " + query.p + " " + query.o + "\n";
			} else {
				output += "null\n";
			}
		}
		return output;
	}

	public String printAllNewQueries() {
		String output = "";
		for (final QueryNode query : queries) {
			if (query != null && query.isNew())
				output += query.getS() + " " + query.p + " " + query.o + "\n";
		}
		return output;
	}

	public void calculateAllNew(ActionContext context) {
		for (final QueryNode q1 : queries) {
			if (q1 != null && !q1.checkedNew()) {
				for (final QueryNode q2 : queries) {
					if (q1.getId() > q2.getId()
							&& q1.isContainedIn(q2, context)) {
						q1.setNew(false);
						break;
					}
				}
				if (!q1.checkedNew())
					q1.setNew(true);
			}
		}
	}

	// public void updateAllFollowingQueries() {
	// for (QueryNode q : queries) {
	// if (q != null && q.isUpdated()) {
	// QueryNode[] nexts = q.next;
	// if (nexts != null) {
	// updateFollowingQueries(nexts);
	// }
	// }
	// }
	// }

	// private void updateFollowingQueries(QueryNode[] queries) {
	// for (QueryNode query : queries) {
	// if (!query.isUpdated()) {
	// query.setUpdated();
	// if (query.next != null) {
	// updateFollowingQueries(query.next);
	// }
	// }
	// }
	// }

	public void resetUpdateCounters() {
		for (QueryNode q : queries) {
			if (q != null)
				q.resetUpdatedCounter();
		}
	}

	public void markAsUpdatedAllSimilarQueries(ActionContext context) {
		for (int i = 0; i < queries.size(); ++i) {
			QueryNode query = queries.get(i);
			if (query != null) {
				for (int j = i + 1; j < queries.size(); ++j) {
					QueryNode query2 = queries.get(j);
					if (query2.isContainedIn(query, context)
							|| query.isContainedIn(query2, context)) {
						if (query.isUpdated() && !query2.isUpdated()) {
							query2.setUpdated();
						} else if (!query.isUpdated() && query2.isUpdated()) {
							query.setUpdated();
						}
					}
				}
			}
		}
	}

	public boolean isThereNewInputForRule(RuleNode ruleNode) {
		QueryNode query = (QueryNode) ruleNode.child;

		while (query != null) {
			if (query.wasUpdatedPreviousItr()) {
				return true;
			}

			QueryNode[] nextQueries = query.next;
			if (nextQueries != null) {
				for (QueryNode nextQuery : nextQueries) {
					if (isThereNewInputForQuery(nextQuery)) {
						return true;
					}
				}
			}

			RuleNode childRule = (RuleNode) query.child;
			while (childRule != null) {
				if (isThereNewInputForRule(childRule)) {
					return true;
				}
				childRule = (RuleNode) childRule.sibling;
			}

			query = (QueryNode) query.sibling;
		}
		return false;
	}

	public QueryNode[] getFirstUpdatedAmongNextQueries(QueryNode query) {
		QueryNode[] array = null;
		if (checkInputForSingleQuery(query)) {
			array = new QueryNode[1];
			array[0] = query;
		} else {
			if (query.next != null) {
				QueryNode[][] arrays = new QueryNode[query.next.length][];
				for (int i = 0; i < query.next.length; ++i) {
					arrays[i] = getFirstUpdatedAmongNextQueries(query.next[i]);
				}

				// Get the total sum
				ArrayList<QueryNode> totalList = new ArrayList<QueryNode>();
				for (QueryNode[] queries : arrays) {
					for (QueryNode tmpQuery : queries) {
						totalList.add(tmpQuery);
					}
				}
				array = totalList.toArray(new QueryNode[totalList.size()]);
			}
		}
		return array;
	}

	private boolean checkInputForSingleQuery(QueryNode query) {
		if (query.child == null) {
			return query.wasUpdatedPreviousItr();
		} else {
			RuleNode rule = (RuleNode) query.child;
			while (rule != null) {
				if (isThereNewInputForRule(rule)) {
					return true;
				}
				rule = (RuleNode) rule.sibling;
			}
			return false;
		}
	}

	public boolean isThereNewInputForQuery(QueryNode query) {
		if (checkInputForSingleQuery(query)) {
			return true;
		}

		QueryNode[] nextQueries = query.next;
		if (nextQueries != null) {
			for (QueryNode nextQuery : nextQueries) {
				if (isThereNewInputForQuery(nextQuery)) {
					return true;
				}
			}
		}

		return false;
	}

	private void markToReExpandQueryAllSubQueries(RuleNode rule) {
		while (rule != null) {
			QueryNode childQuery = (QueryNode) rule.child;
			while (childQuery != null) {
				childQuery.setReExpansion();
				markToReExpandQueryAllSubQueries((RuleNode) childQuery.child);
				QueryNode nextQuery = childQuery.next != null ? childQuery.next[0]
						: null;
				while (nextQuery != null) {
					nextQuery.setReExpansion();
					markToReExpandQueryAllSubQueries((RuleNode) nextQuery.child);
					nextQuery = nextQuery.next != null ? nextQuery.next[0]
							: null;
				}
				childQuery = (QueryNode) childQuery.sibling;
			}

			rule = (RuleNode) rule.sibling;
		}
	}

	public void setToReexpand(QueryNode newQuery) throws Exception {
		if (newQuery.isNew()) {
			newQuery.setReExpansion();
			markToReExpandQueryAllSubQueries((RuleNode) newQuery.child);
		} else {
			throw new Exception("Not supported");
		}
	}

	// public void addTupleSetToCache(TupleSet set, QueryNode head, RuleNode
	// rule,
	// QueryNode childQuery) {
	// if (isListAlreadyComputed(head, rule, childQuery) != null) {
	// return;
	// }
	// ListInfo info = new ListInfo();
	// info.set = set;
	// info.head = head;
	// info.rule = rule;
	// info.query = childQuery;
	// cacheList.add(info);
	// }
	//
	// public TupleSet isListAlreadyComputed(QueryNode head, RuleNode rule,
	// QueryNode childQuery) {
	// for (ListInfo info : cacheList) {
	// if (rule.rule.id == info.rule.rule.id
	// && childQuery.getS() == info.query.getS()
	// && childQuery.p == info.query.p
	// && childQuery.o == info.query.o) {
	// // Check if the head is compatible
	// long headS = head.getS();
	// long infoHeadS = info.head.getS();
	// if (headS < 0 && infoHeadS < 0 || headS == infoHeadS) {
	// long headP = head.p;
	// long infoHeadP = info.head.p;
	// if (headP < 0 && infoHeadP < 0 || headP == infoHeadP) {
	// long headO = head.o;
	// long infoHeadO = info.head.o;
	// if (headO < 0 && infoHeadO < 0 || headO == infoHeadO) {
	// return info.set;
	// }
	// }
	// }
	// }
	// }
	// return null;
	// }
	//
	// public void resetListCache() {
	// cacheList.clear();
	// }
}
