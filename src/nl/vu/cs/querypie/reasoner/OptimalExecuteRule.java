package nl.vu.cs.querypie.reasoner;

//import nl.vu.cs.ajira.actions.Action;
//import nl.vu.cs.ajira.actions.ActionConf;
//import nl.vu.cs.ajira.actions.ActionContext;
//import nl.vu.cs.ajira.actions.ActionFactory;
//import nl.vu.cs.ajira.actions.ActionOutput;
//import nl.vu.cs.ajira.actions.ActionSequence;
//import nl.vu.cs.ajira.data.types.Tuple;
//import nl.vu.cs.querypie.reasoning.expand.QSQEvaluateQuery;
//import nl.vu.cs.querypie.reasoning.expand.QueryNode;
//import nl.vu.cs.querypie.reasoning.expand.RuleNode;
//import nl.vu.cs.querypie.reasoning.expand.Tree;
//import nl.vu.cs.querypie.storage.RDFTerm;
//import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

//public class OptimalExecuteRule extends Action {
//
//	public static final int I_RULENODEID = 0;
//	public static final int S_TREENAME = 1;
//
//	private InMemoryTripleContainer outputContainer;
//	private long previousSize;
//
//	public static final void applyTo(ActionSequence sequence,
//			RuleNode ruleNode, boolean childExplicit, Tree t, String treename,
//			ActionContext context) throws Exception {
//
//		// Evaluate child query with qsq
//		QueryNode childQuery = (QueryNode) ruleNode.child;
//		QSQEvaluateQuery.evaluateQuery(sequence, t, childQuery, childExplicit,
//				context);
//
//		// Apply the rule
//		ReasoningUtils.applyRule(sequence, ruleNode.rule,
//				(QueryNode) ruleNode.parent, childQuery, ruleNode.strag_id,
//				ruleNode.ref_memory, ruleNode.current_pattern, treename,
//				childQuery.list_head, childQuery.list_id,
//				ruleNode.idFilterValues);
//
//		ActionConf c = ActionFactory.getActionConf(OptimalExecuteRule.class);
//		c.setParamInt(I_RULENODEID, ruleNode.getId());
//		c.setParamString(S_TREENAME, treename);
//		sequence.add(c);
//	}
//
//	@Override
//	protected void registerActionParameters(ActionConf conf) {
//		conf.registerParameter(I_RULENODEID, "I_RULENODEID", 0, true);
//		conf.registerParameter(S_TREENAME, "S_TREENAME", null, true);
//	}
//
//	@Override
//	public void startProcess(ActionContext context) throws Exception {
//		outputContainer = null;
//		previousSize = 0;
//	}
//
//	@Override
//	public void process(Tuple tuple, ActionContext context,
//			ActionOutput actionOutput) throws Exception {
//		// Collect all the triples in a temporary container
//		if (outputContainer == null) {
//			outputContainer = (InMemoryTripleContainer) context
//					.getObjectFromCache("OptimalExecuteRule");
//			if (outputContainer == null) {
//				outputContainer = new InMemoryTripleContainer();
//				context.putObjectInCache("OptimalExecuteRule", outputContainer);
//			}
//			previousSize = outputContainer.size();
//		}
//
//		if (outputContainer.addTriple((RDFTerm) tuple.get(0),
//				(RDFTerm) tuple.get(1), (RDFTerm) tuple.get(2), null)) {
//			actionOutput.output(tuple);
//		}
//	}
//
//	@Override
//	public void stopProcess(ActionContext context, ActionOutput actionOutput)
//			throws Exception {
//		// Should I repeat the process?
//		if (outputContainer != null && outputContainer.size() != previousSize) {
//			ActionSequence actions = new ActionSequence();
//			String treeName = getParamString(S_TREENAME);
//			Tree t = (Tree) context.getObjectFromCache(treeName);
//			RuleNode rn = t.getRule(getParamInt(I_RULENODEID));
//			applyTo(actions, rn, false, t, treeName, context);
//			actionOutput.branch(actions);
//		} else {
//			context.putObjectInCache("OptimalExecuteRule", null);
//			outputContainer = null;
//		}
//	}
//}