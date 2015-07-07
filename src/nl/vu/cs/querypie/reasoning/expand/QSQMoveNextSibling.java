package nl.vu.cs.querypie.reasoning.expand;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.QSQBCAlgo;
import nl.vu.cs.querypie.storage.RDFTerm;

public class QSQMoveNextSibling extends Action {

	public static final int I_NODE_ID = 0;
	public static final int B_RULE = 1;

	private int nodeId;
	private boolean ruleNode;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_NODE_ID, "I_QUERY_ID", null, true);
		conf.registerParameter(B_RULE, "B_RULE", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		nodeId = getParamInt(I_NODE_ID);
		ruleNode = getParamBoolean(B_RULE);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple); // Forward the tuples
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// Get the sibling of the query in query id and evaluate it if it's not
		// already evaluated.
		Tree tree = (Tree) context.getObjectFromCache(QSQBCAlgo.TREE_ID);
		ActionSequence actions = new ActionSequence();
		if (ruleNode) {
			RuleNode rn = tree.getRule(nodeId);
			QueryNode query = (QueryNode) rn.child;
			QSQEvaluateQuery.applyRule(actions, tree, (QueryNode) rn.parent,
					rn, query, query.list_id, rn.idFilterValues, context);

			if (rn.sibling != null) {
				ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName(), TBoolean.class.getName());
				actions.add(c);

				c = ActionFactory.getActionConf(QSQMoveNextSibling.class);
				c.setParamInt(QSQMoveNextSibling.I_NODE_ID, rn.sibling.getId());
				c.setParamBoolean(QSQMoveNextSibling.B_RULE, true);
				actions.add(c);
			}

		} else {
			QueryNode qn = tree.getQuery(nodeId);
			QSQEvaluateQuery.evaluateQuery(actions, tree, qn, true, context);

			if (qn.sibling != null) {
				ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName(), TBoolean.class.getName());
				actions.add(c);

				c = ActionFactory.getActionConf(QSQMoveNextSibling.class);
				c.setParamInt(QSQMoveNextSibling.I_NODE_ID, qn.sibling.getId());
				c.setParamBoolean(QSQMoveNextSibling.B_RULE, false);
				actions.add(c);
			}
		}
		actionOutput.branch(actions);
	}
}