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
	public static final int I_TYPE_RULES = 3;
	public static final int B_FORCERESTART = 4;

	private boolean explicit;
	private boolean recursive;
	private boolean cacheInput;
	private int typeRules;
	private Queue rulesQueue;
	private boolean forceRestart;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_EXPLICIT, "B_EXPLICIT", false, false);
		conf.registerParameter(B_ALLOWRECURSION, "B_ALLOWRECURSION", true,
				false);
		conf.registerParameter(B_CACHEINPUT, "B_CACHEINPUT", true, false);
		conf.registerParameter(I_TYPE_RULES, "I_TYPE_RULES", TreeExpander.ALL,
				false);
		conf.registerParameter(B_FORCERESTART, "B_FORCERESTART", false, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		explicit = getParamBoolean(B_EXPLICIT);
		recursive = getParamBoolean(B_ALLOWRECURSION);
		cacheInput = getParamBoolean(B_CACHEINPUT);
		typeRules = getParamInt(I_TYPE_RULES);
		forceRestart = getParamBoolean(B_FORCERESTART);
		if (typeRules == TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS) {
			rulesQueue = (Queue) context.getObjectFromCache("queue");
			if (rulesQueue == null) {
				synchronized (Queue.class) {
					rulesQueue = (Queue) context.getObjectFromCache("queue");
					if (rulesQueue == null) {
						rulesQueue = new Queue();
						context.putObjectInCache("queue", rulesQueue);
					}
				}
			}
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		// Get the query in input and expand it.
		final int queryId = ((TInt) tuple.get(3)).getValue();
		Tree t = (Tree) context.getObjectFromCache("tree");
		QueryNode q = null;
		if (queryId == 0 && (t == null || forceRestart)) {
			// new tree
			t = new Tree();
			context.putObjectInCache("tree", t);
			q = t.newRoot();
			q.setS(((RDFTerm) tuple.get(0)).getValue());
			q.p = ((RDFTerm) tuple.get(1)).getValue();
			q.o = ((RDFTerm) tuple.get(2)).getValue();
		} else {
			q = t.getQuery(queryId);
		}

		if (explicit && q.wasUpdatedPreviousItr()) {
			final ActionSequence actions = new ActionSequence();
			ReasoningUtils.getResultsQuery(actions, tuple, false);
			actions.add(ActionFactory.getActionConf(SetAsExplicit.class));
			actionOutput.branch(actions);
		}

		if (!q.isExpanded()) {
			TreeExpander.expandQuery(context, q, t, typeRules, null);
			q.setExpanded();
		} else if (q.shouldBeReExpanded()) {
			TreeExpander.reExpandQuery(context, q, t, typeRules);
			q.setExpanded();
		}

		RuleNode ruleNode = (RuleNode) q.child;
		while (ruleNode != null) {
			if (t.isThereNewInputForRule(ruleNode)) {
				if (typeRules != TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS
						|| ruleNode.rule.type == 1 || ruleNode.rule.type == 2) {
					QueryNode childQuery = (QueryNode) ruleNode.child;
					while (childQuery != null) {
						if (t.isThereNewInputForQuery(childQuery)) {
							QueryNode[] qns = t
									.getFirstUpdatedAmongNextQueries(childQuery);
							for (QueryNode qn : qns) {
								ReasoningUtils.generate_new_chain(actionOutput,
										ruleNode.rule, ruleNode.strag_id,
										ruleNode.rule.type == 3
												|| ruleNode.rule.type == 4, qn,
										qn.current_pattern, q, context,
										qn.list_id, recursive, cacheInput,
										typeRules, "tree",
										ruleNode.idFilterValues);
							}
						}
						childQuery = (QueryNode) childQuery.sibling;
					}
				} else if (typeRules == TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS) {
					QueryNode childQuery = (QueryNode) ruleNode.child;
					while (childQuery != null) {
						if (t.isThereNewInputForQuery(childQuery)) {
							childQuery.setQueued(true);
							rulesQueue.addNode(childQuery);
						}
						childQuery = (QueryNode) childQuery.sibling;
					}
				}
			}

			// next rule
			ruleNode = (RuleNode) ruleNode.sibling;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		rulesQueue = null;
	}
}