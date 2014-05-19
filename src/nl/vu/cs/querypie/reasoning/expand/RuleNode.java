package nl.vu.cs.querypie.reasoning.expand;

import nl.vu.cs.querypie.reasoner.rules.Rule;

public class RuleNode extends Node {
	public final Rule rule;
	public boolean single_node = false;
	public int strag_id = -1;

	public int ref_memory = -1;
	public int current_pattern = 0;

	public RuleNode(Node parent, Rule rule, int id, Tree tree) {
		super(id, tree);
		this.parent = parent;
		this.rule = rule;
		height = parent.height + 1;
	}
}