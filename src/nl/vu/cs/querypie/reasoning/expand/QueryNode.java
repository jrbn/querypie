package nl.vu.cs.querypie.reasoning.expand;

import java.util.Collection;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryNode extends Node {

	protected static final Logger log = LoggerFactory
			.getLogger(QueryNode.class);

	private long s;
	public long p, o;
	String nameS, nameP, nameO;

	public Collection<Long> list_heads = null;
	public int list_id = -1;

	private boolean isComputed = false;
	private boolean checkComputed = false;

	private boolean isExpanded = false;
	private boolean shouldBeReExpanded = false;

	private boolean isNew = true;
	private boolean isNewChecked = false;

	private boolean isQueued = false;

	// Used in the algo OptimalRuleBC
	public int idFilterValues = -1;
	public int posFilterValues = -1;

	public QueryNode prev = null;
	public QueryNode[] next = null;
	public int current_pattern = 0;

	private long timeUpdate = -1;
	private boolean updatedPreviousIteration = true;

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

	public final void setUpdated() {
		if (timeUpdate == -1) {
			synchronized (this) {
				if (timeUpdate == -1)
					timeUpdate = System.currentTimeMillis();
			}
		}
	}

	public final boolean isUpdated() {
		return timeUpdate != -1;
	}

	public final void resetUpdatedCounter() {
		updatedPreviousIteration = isUpdated();
		timeUpdate = -1;
	}

	public final void forceUpdateInPreviousIteration() {
		updatedPreviousIteration = true;
	}

	public final boolean wasUpdatedPreviousItr() {
		return updatedPreviousIteration;
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

	public void setQueued(boolean value) {
		isQueued = value;
	}

	public boolean isQueued() {
		return isQueued;
	}

	public boolean checkedNew() {
		return isNewChecked;
	}

	public boolean isNew() {
		return isNew;
	}

	public final void setS(final long s) {
		this.s = s;
	}

	public final long getS() {
		return s;
	}

	public boolean equalsInAncestors(ActionContext context) {
		QueryNode parent = null;
		if (this.parent != null) {
			parent = (QueryNode) this.parent.parent; // skip rule node
		}

		final RDFTerm ts = new RDFTerm(s);
		final RDFTerm parentTerm = new RDFTerm();

		while (parent != null) {
			// Check whether the query is the same
			parentTerm.setValue(parent.s);
			if (parentTerm.equals(ts, context)) {
				parentTerm.setValue(parent.p);
				final RDFTerm tp = new RDFTerm(p);
				if (parentTerm.equals(tp, context)) {
					parentTerm.setValue(parent.o);
					final RDFTerm to = new RDFTerm(o);
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
			if (tree.isComputed(this, context)) {
				setComputed();
			} else {
				checkComputed = true;
				isComputed = false;
			}
		}
		return isComputed;
	}

	@Override
	public String toString() {
		String parentRule = null;
		if (parent != null) {
			parentRule = ((RuleNode) parent).rule.toString();
		}
		return s + " " + p + " " + o + ". ID " + getId() + " height " + height
				+ " parentRule " + parentRule;
	}

	public boolean isNew(final ActionContext context) {
		if (!isNewChecked) {
			isNew = tree.isNew(this, context);
			isNewChecked = true;
		}
		return isNew;
	}

	public boolean isExpanded() {
		return s == Schema.SCHEMA_SUBSET || isExpanded;
	}

	public void setNew(boolean value) {
		isNew = value;
		isNewChecked = true;
	}

	public void setExpanded() {
		isExpanded = true;
		shouldBeReExpanded = false;
	}

	public void setToExpand() {
		isExpanded = false;
	}

	@SuppressWarnings("unchecked")
	private final static boolean isTermContainedIn(long term1, long term2,
			ActionContext context) {
		if (term1 == term2) {
			return true;
		}
		// One of the two terms is either ALL_RESOURCES or a set.
                // No, could be SCHEMA_SUBSET as well. Added checks. --Ceriel
		if (term1 >= 0) {
			if (term2 >= 0 || term2 == Schema.SCHEMA_SUBSET) {
				// we already know they are not equal
                                return false;
			} else if (term2 != Schema.ALL_RESOURCES) {
				final Collection<Long> set = Schema.getInstance().getSubset(
						term2, context);
				return set.contains(term1);
			}
		} else if (term1 == Schema.ALL_RESOURCES) {
			if (term2 != Schema.ALL_RESOURCES) {
				return false;
			}
		} else if (term1 == Schema.SCHEMA_SUBSET) {
                        return false;
                } else { // Here term1 is a set
			final Collection<Long> set = Schema.getInstance().getSubset(term1,
					context);
			if (term2 >= 0) {
				if (set.size() == 1) {
					return set.contains(term2);
				} else {
					return false;
				}
			} else if (term2 != Schema.ALL_RESOURCES) {
				// final Collection<Long> set2 =
				// Schema.getInstance().getSubset(term2, context);
				// Is this worth-while? It is an expensive test,
				// how often does it actually reduce work later on? --Ceriel
				// return set2.containsAll(set);
				return false;
			}
		}
		return true;
	}

	public final boolean isContainedIn(final QueryNode parentQuery,
			final ActionContext context) {
		// Filter out easy cases before dealing with sets.
		if (s >= 0 && parentQuery.s >= 0 && s != parentQuery.s) {
			return false;
		}
		if (p >= 0 && parentQuery.p >= 0 && p != parentQuery.p) {
			return false;
		}
		if (o >= 0 && parentQuery.o >= 0 && o != parentQuery.o) {
			return false;
		}
		return isTermContainedIn(s, parentQuery.s, context)
				&& isTermContainedIn(p, parentQuery.p, context)
				&& isTermContainedIn(o, parentQuery.o, context);
	}

	public boolean shouldBeReExpanded() {
		return shouldBeReExpanded;
	}

	public void setReExpansion() {
		shouldBeReExpanded = true;
	}
}
