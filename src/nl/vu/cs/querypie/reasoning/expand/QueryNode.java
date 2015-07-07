package nl.vu.cs.querypie.reasoning.expand;

import java.util.Set;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

public class QueryNode extends Node {

	public long s, p, o;
	String nameS, nameP, nameO;

	public long list_head = -1;
	public int list_id = -1;

	private boolean isComputed = false;
	private boolean checkComputed = false;

	private boolean isExpanded = false;

	QueryNode(Node parent, int id, Tree tree) {
		super(id, tree);
		this.parent = parent;
		sibling = null;
		child = null;

		if (parent != null)
			height = parent.height + 1;
	}

	public final void setTerm(final int pos, final long v) {
		switch (pos) {
		case 0:
			s = v;
			break;
		case 1:
			p = v;
			break;
		case 2:
			o = v;
			break;
		}
	}

	public final long getTerm(final int pos) {
		switch (pos) {
		case 0:
			return s;
		case 1:
			return p;
		case 2:
			return o;
		}
		return -1;
	}

	public void setName(int pos, String v) {
		switch (pos) {
		case 0:
			nameS = v;
			break;
		case 1:
			nameP = v;
			break;
		case 2:
			nameO = v;
			break;
		}
	}

	public boolean equalsInAncestors(ActionContext context) {
		QueryNode parent = null;
		if (this.parent != null) {
			parent = (QueryNode) this.parent.parent; // skip rule node
		}

		RDFTerm ts = new RDFTerm(s);
		RDFTerm parentTerm = new RDFTerm();

		while (parent != null) {
			// Check whether the query is the same
			parentTerm.setValue(parent.s);
			if (parentTerm.equals(ts, context)) {
				parentTerm.setValue(parent.p);
				RDFTerm tp = new RDFTerm(p);
				if (parentTerm.equals(tp, context)) {
					parentTerm.setValue(parent.o);
					RDFTerm to = new RDFTerm(o);
					if (parentTerm.equals(to, context)) {
						return true;
					}
				}
			}

			if (parent.parent != null) {
				parent = (QueryNode) parent.parent.parent; // skip rule node
			} else {
				parent = null;
			}
		}

		return false;
	}

	public void setComputed() {
		isComputed = true;
		checkComputed = true;
		tree.addToComputed(this);
	}

	public boolean isComputed(final ActionContext context) {
		if (s == Schema.SCHEMA_SUBSET || isComputed) {
			return true;
		} else if (!checkComputed) {
			// Check all the previous computed queries to see whether we have
			// already precomputed it
			isComputed = tree.isComputed(this, context);
			checkComputed = true;
		}

		return isComputed;
	}

	public boolean isExpanded() {
		return s == Schema.SCHEMA_SUBSET || isExpanded;
	}

	public void setExpanded() {
		isExpanded = true;
	}

	@SuppressWarnings("unchecked")
	private final static boolean isTermContainedIn(long term1, long term2, ActionContext context) {
		// Check the subject
		if (term1 >= 0) {
			if (term2 >= 0) {
				return term1 == term2;
			} else if (term2 != Schema.ALL_RESOURCES) {				
				Set<Long> set = (Set<Long>) context.getObjectFromCache(term2);
				return set.contains(term1);
			}
		} else if (term1 == Schema.ALL_RESOURCES) {
			if (term2 != Schema.ALL_RESOURCES) {
				return false;
			}
		} else { //Here term1 is a set
			Set<Long> set = (Set<Long>) context.getObjectFromCache(term1);
			if (term2 >= 0) {
				if (set.size() == 1) {
					return set.contains(term2);
				} else {
					return false;
				}
			} else if (term2 != Schema.ALL_RESOURCES) {
				Set<Long> set2 = (Set<Long>) context.getObjectFromCache(term2);
				return set.equals(set2);
			}			
		}
		return true;
	}

	public final boolean isContainedIn(final QueryNode parentQuery, final ActionContext context) {
		return isTermContainedIn(s, parentQuery.s, context)
				&& isTermContainedIn(p, parentQuery.p, context)
				&& isTermContainedIn(o, parentQuery.o, context);
	}
}
