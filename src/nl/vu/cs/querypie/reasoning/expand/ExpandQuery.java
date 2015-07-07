package nl.vu.cs.querypie.reasoning.expand;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoner.SetAsExplicit;
import nl.vu.cs.querypie.storage.RDFTerm;

public class ExpandQuery extends Action {

	public static final int B_EXPLICIT = 0;
	public static final int B_ALLOWRECURSION = 1;
	public static final int B_CACHEINPUT = 2;
	public static final int B_ONLY_FIRST_AND_SECOND_RULES = 3;

	private boolean explicit;
	private boolean recursive;
	private boolean cacheInput;
	private boolean onlyFirstSecond;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_EXPLICIT, "B_EXPLICIT", false, false);
		conf.registerParameter(B_ALLOWRECURSION, "B_ALLOWRECURSION", true,
				false);
		conf.registerParameter(B_CACHEINPUT, "B_CACHEINPUT", true, false);
		conf.registerParameter(B_ONLY_FIRST_AND_SECOND_RULES,
				"B_ONLY_FIRST_AND_SECOND_RULES", false, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		explicit = getParamBoolean(B_EXPLICIT);
		recursive = getParamBoolean(B_ALLOWRECURSION);
		cacheInput = getParamBoolean(B_CACHEINPUT);
		onlyFirstSecond = getParamBoolean(B_ONLY_FIRST_AND_SECOND_RULES);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		if (explicit) {
			ActionSequence actions = new ActionSequence();
			ReasoningUtils.getResultsQuery(actions, tuple, false);
			actions.add(ActionFactory.getActionConf(SetAsExplicit.class));
			actionOutput.branch(actions);
		}

		// Get the query in input and expand it.
		int queryId = ((TInt) tuple.get(3)).getValue();

		Tree t = null;
		QueryNode q = null;
		if (queryId == 0) {
			// new tree
			t = new Tree();
			q = t.newRoot();
			context.putObjectInCache("tree", t);
			q.s = ((RDFTerm) tuple.get(0)).getValue();
			q.p = ((RDFTerm) tuple.get(1)).getValue();
			q.o = ((RDFTerm) tuple.get(2)).getValue();
		} else {
			// get the querynode
			t = (Tree) context.getObjectFromCache("tree");
			q = t.getQuery(queryId);
		}

		TreeExpander.expandQuery(context, q, t,
				onlyFirstSecond ? TreeExpander.ONLY_FIRST_SECOND
						: TreeExpander.ALL);

		RuleNode ruleNode = (RuleNode) q.child;
		while (ruleNode != null) {
			QueryNode qn = (QueryNode) ruleNode.child;
			while (qn != null) {

				ReasoningUtils.generate_new_chain(actionOutput, ruleNode.rule,
						ruleNode.strag_id, ruleNode.rule.type == 3
								|| ruleNode.rule.type == 4, qn,
						ruleNode.current_pattern, q, ruleNode.ref_memory,
						context, qn.list_head, qn.list_id,
						recursive, cacheInput);
				qn = (QueryNode) qn.sibling;
			}

			// next rule
			ruleNode = (RuleNode) ruleNode.sibling;
		}
	}

}