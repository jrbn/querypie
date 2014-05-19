package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ExpandTree extends Action {

	public static final int I_HEIGHT = 0;
	public static final String FINISHED_EXPANSION = "TreeExpander.finishedIncrementalExpansion";

	private int height;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_HEIGHT, "I_HEIGHT", 0, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		height = getParamInt(I_HEIGHT);
	}

	private ActionSequence getActionSequenceFromParents(ActionContext context,
			QueryNode node) throws Exception {
		List<Node> parentNodes = getAllParentNodes(node);
		ActionSequence parentSequence = new ActionSequence();
		// Create a chain with all the parent rules
		for (int j = parentNodes.size() - 2; j >= 0; j -= 2) {
			RuleNode ruleNode = (RuleNode) parentNodes.get(j);
			Rule rule = ruleNode.rule;
			QueryNode parent = (QueryNode) parentNodes.get(j + 1);
			QueryNode child = (QueryNode) parentNodes.get(j - 1);
			ReasoningUtils.generate_new_chain(parentSequence, rule,
					ruleNode.strag_id, rule.type == 3 || rule.type == 4, child,
					ruleNode.current_pattern, parent, ruleNode.ref_memory,
					context, child.list_head, child.list_id, false, true);
		}
		return parentSequence;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		Tree tree = (Tree) context.getObjectFromCache("tree");
		// Go through all the queries and expand only the ones at a certain
		// height
		boolean expansionDone = false;
		for (int i = tree.getNQueries() - 1; i >= 0; --i) {
			QueryNode node = tree.getQuery(i);
			if (node.height == height) {
				// Expand it.
				TreeExpander.expandQuery(context, node, tree,
						TreeExpander.ONLY_FIRST_SECOND);

				if (node.child != null) {
					// Translate all the children of this query in new chains to
					// execute. Need to add all the parent rules of course!
					ActionSequence parentSequence = getActionSequenceFromParents(
							context, node);

					// Add the child query
					RuleNode ruleNode = (RuleNode) node.child;
					while (ruleNode != null) {
						Rule rule = ruleNode.rule;
						QueryNode query = (QueryNode) ruleNode.child;

						while (query != null) {
							// Add a new query
							ActionSequence newSequence = new ActionSequence();
							parentSequence.copyTo(newSequence);
							ReasoningUtils
									.generate_new_chain(parentSequence, rule,
											ruleNode.strag_id, rule.type == 3
													|| rule.type == 4, query,
											ruleNode.current_pattern,
											(QueryNode) ruleNode.parent,
											ruleNode.ref_memory, context,
											query.list_head, query.list_id,
											false, true);
							actionOutput.branch(newSequence);
							query = (QueryNode) query.sibling;
						}
						ruleNode = (RuleNode) ruleNode.sibling;
					}

					expansionDone = true;
				}
			}
		}

		// If no expansion is possible, then reactivate also the other rules and
		// proceed with a traditional expansion.
		if (!expansionDone) {
			int nqueries = tree.getNQueries();
			for (int i = 0; i < nqueries; ++i) {
				QueryNode node = tree.getQuery(i);
				TreeExpander.expandQuery(context, node, tree,
						TreeExpander.ONLY_THIRD_FOURTH);
				RuleNode ruleNode = (RuleNode) (node.child != null ? node.child
						: null);
				ActionSequence parentSequence = null;
				while (ruleNode != null
						&& (ruleNode.rule.type == 3 || ruleNode.rule.type == 4)) {

					if (parentSequence == null) {
						parentSequence = getActionSequenceFromParents(context,
								node);
					}

					// For each query output a new chain
					QueryNode query = (QueryNode) ruleNode.child;
					while (query != null) {
						ActionSequence newSequence = new ActionSequence();
						parentSequence.copyTo(newSequence);
						ReasoningUtils.generate_new_chain(newSequence,
								ruleNode.rule, ruleNode.strag_id, true, query,
								ruleNode.current_pattern, node,
								ruleNode.ref_memory, context, query.list_head,
								query.list_id, true, true);
						actionOutput.branch(newSequence);

						query = (QueryNode) query.sibling;
					}
					ruleNode = (RuleNode) ruleNode.sibling;
				}
			}
			context.putObjectInCache(FINISHED_EXPANSION, "true");
		}

	}

	private List<Node> getAllParentNodes(QueryNode node) {
		ArrayList<Node> nodes = new ArrayList<Node>();
		Node n = node;
		while (n != null) {
			nodes.add(n);
			n = n.parent;
		}
		return nodes;
	}
}