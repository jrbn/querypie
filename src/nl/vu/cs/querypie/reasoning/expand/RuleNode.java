package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;

import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.memory.TupleSet;

public class RuleNode extends Node {
	public final Rule rule;
	public boolean single_node = false;
	public int strag_id = -1;

	// Used in the algo OptimalRuleBC
	public int idFilterValues = -1;
	public int posFilterValues = -1;

	public TupleSet[] intermediateTuples;
	public TupleSet[] listIntermediateTuples; // Map list_head to

	final public ArrayList<Long> cacheIDs = new ArrayList<Long>();

	public RuleNode(Node parent, Rule rule, int id, Tree tree) {
		super(id, tree);
		this.parent = parent;
		this.rule = rule;
		height = parent.height + 1;
	}

	public boolean equalsTo(RuleNode output) {
		return output.rule.id == rule.id && single_node == output.single_node
				&& strag_id == output.strag_id;
	}
}