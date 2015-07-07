package nl.vu.cs.querypie.reasoning.expand;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.querypie.reasoner.QSQBCAlgo;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoner.SetAsExplicit;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor1;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor3;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor4;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

public class QSQEvaluateQuery extends Action {

	public static final int I_QUERY_ID = 0;

	private int id;
	private InMemoryTripleContainer outputContainer;
	private long previousSize;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_QUERY_ID, "I_QUERY_ID", -1, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		id = getParamInt(I_QUERY_ID);
		outputContainer = null;
		previousSize = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// Collect all the triples in a temporary container
		if (outputContainer == null) {
			outputContainer = (InMemoryTripleContainer) context
					.getObjectFromCache("qsq-" + id);
			if (outputContainer == null) {
				outputContainer = new InMemoryTripleContainer();
				context.putObjectInCache("qsq-" + id, outputContainer);
			}
			previousSize = outputContainer.size();
		}

		if (outputContainer.addTriple((RDFTerm) tuple.get(0),
				(RDFTerm) tuple.get(1), (RDFTerm) tuple.get(2), null)) {
			actionOutput.output(tuple);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// Should I repeat the process?
		if (outputContainer != null && outputContainer.size() != previousSize) {
			ActionSequence actions = new ActionSequence();
			Tree t = (Tree) context.getObjectFromCache(QSQBCAlgo.TREE_ID);
			evaluateQuery(actions, t, t.getQuery(id), false, context);
			actionOutput.branch(actions);
		} else {
			context.putObjectInCache("qsq-" + id, null);
			Tree t = (Tree) context.getObjectFromCache(QSQBCAlgo.TREE_ID);
			QueryNode q = t.getQuery(id);
			q.setComputed();

			// Copy the data to the intermediate container used by RDFStorage
			if (outputContainer == null) {
				outputContainer = (InMemoryTripleContainer) context
						.getObjectFromCache("qsq-" + id);
			}
			if (outputContainer != null && outputContainer.size() > 0) {
				InMemoryTripleContainer intermediateTriples = (InMemoryTripleContainer) context
						.getObjectFromCache("inputIntermediateTuples");
				if (intermediateTriples == null) {
					intermediateTriples = outputContainer;
					context.putObjectInCache("inputIntermediateTuples",
							intermediateTriples);
				} else {
					intermediateTriples.addAll(outputContainer);
				}
				// add the query
				intermediateTriples.addQuery(q.s, q.p, q.o, context, null);
				intermediateTriples.index();
			}
		}
		outputContainer = null;
	}

	public static final void evaluateRootQuery(ActionSequence actions, long v1,
			long v2, long v3, ActionContext context) throws Exception {

		Tree t = new Tree();
		QueryNode q = t.newQuery(null);
		q.s = v1;
		q.p = v2;
		q.o = v3;
		context.putObjectInCache(QSQBCAlgo.TREE_ID, t);
		evaluateQuery(actions, t, q, true, context);
	}

	public static final void evaluateQuery(ActionSequence actions, Tree t,
			QueryNode q, boolean explicit, ActionContext context)
			throws Exception {

		// 1- Expand the query
		if (!q.isExpanded() && !q.isComputed(context)) {
			TreeExpander.expandQuery(context, q, t, TreeExpander.ALL);
			q.setExpanded();
		}

		if (explicit) {
			if (q.s == Schema.SCHEMA_SUBSET) {
				ActionConf c = ActionFactory
						.getActionConf(QueryInputLayer.class);
				c.setParamString(QueryInputLayer.S_INPUTLAYER,
						DummyLayer.class.getName());
				c.setParamWritable(
						QueryInputLayer.W_QUERY,
						new nl.vu.cs.ajira.actions.support.Query(TupleFactory
								.newTuple(new RDFTerm(q.s), new RDFTerm(q.p),
										new RDFTerm(q.o), new TInt(q.getId()))));
				actions.add(c);
			} else { // Read from the default layer
				ReasoningUtils.getResultsQuery(actions, TupleFactory.newTuple(
						new RDFTerm(q.s), new RDFTerm(q.p), new RDFTerm(q.o)),
						false);
				actions.add(ActionFactory.getActionConf(SetAsExplicit.class));
			}
		} else {
			QueryNode child = (QueryNode) q.child.child;
			applyRule(actions, t, q, (RuleNode) q.child, child,
					child.list_head, child.list_id, context);
		}

		boolean shouldRepeat = !q.isComputed(context) && q.child != null;
		if (shouldRepeat) {
			// Add the move to sibling action (if there are siblings)
			if (explicit) {
				ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName(), TBoolean.class.getName());
				actions.add(c);

				c = ActionFactory.getActionConf(QSQMoveNextSibling.class);
				c.setParamInt(QSQMoveNextSibling.I_NODE_ID, q.child.getId());
				c.setParamBoolean(QSQMoveNextSibling.B_RULE, true);
				actions.add(c);
			} else if (q.child.sibling != null) {
				ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName(), TBoolean.class.getName());
				actions.add(c);

				c = ActionFactory.getActionConf(QSQMoveNextSibling.class);
				c.setParamInt(QSQMoveNextSibling.I_NODE_ID,
						q.child.sibling.getId());
				c.setParamBoolean(QSQMoveNextSibling.B_RULE, true);
				actions.add(c);
			}

			// Collect everything on one node
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			actions.add(c);

			// Repeat the process adding QSQAlgo
			c = ActionFactory.getActionConf(QSQEvaluateQuery.class);
			c.setParamInt(I_QUERY_ID, q.getId());
			actions.add(c);
		} else {
			// Mark it as computed
			if (!q.isComputed(context)) {
				q.setComputed();
			}
			
			//This query was either being computed or no reasoning could be applied. In the second case,
			//A query to the datalayer might return some duplicates that we need to remove.
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);			
			actions.add(c);
			
			c = ActionFactory.getActionConf(RemoveDuplicates.class);
			actions.add(c);
		}
	}

	public static final void applyRule(ActionSequence newChain, Tree tree,
			QueryNode head, RuleNode rule, QueryNode query, long list_head,
			int list_id, ActionContext context) throws Exception {
		applyRule(newChain, tree, head, rule.rule, rule.single_node,
				rule.strag_id, rule.ref_memory, rule.current_pattern, query,
				list_head, list_id, context);
	}

	public static final void applyRule(ActionSequence newChain, Tree tree,
			QueryNode head, Rule rule, boolean single_node, int strag_id,
			int ref_memory, int current_pattern, QueryNode query,
			long list_head, int list_id, ActionContext context)
			throws Exception {

		evaluateQuery(newChain, tree, query, true, context);

		if (query.sibling != null) {
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			newChain.add(c);

			c = ActionFactory.getActionConf(QSQMoveNextSibling.class);
			c.setParamInt(QSQMoveNextSibling.I_NODE_ID, query.sibling.getId());
			c.setParamBoolean(QSQMoveNextSibling.B_RULE, false);
			newChain.add(c);
		}

		if (single_node) {
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			newChain.add(c);
		}

		ActionConf c = ActionFactory
				.getActionConf(ReasoningUtils.ruleClasses[rule.type].getName());
		c.setParamInt(RuleExecutor1.I_RULEDEF, rule.id);
		c.setParamLong(RuleExecutor1.L_FIELD1, head.s);
		c.setParamLong(RuleExecutor1.L_FIELD2, head.p);
		c.setParamLong(RuleExecutor1.L_FIELD3, head.o);
		if (rule.type > 2) {
			c.setParamInt(RuleExecutor3.I_STRAG_ID, strag_id);
			c.setParamInt(RuleExecutor3.I_KEY, ref_memory);
			c.setParamInt(RuleExecutor3.I_PATTERN_POS, current_pattern);
			c.setParamInt(RuleExecutor3.I_QUERY_ID, query.getId());
		}
		if (rule.type == 4) {
			c.setParamLong(RuleExecutor4.L_LIST_HEAD, list_head);
			c.setParamInt(RuleExecutor4.I_LIST_CURRENT, list_id);
		}
		newChain.add(c);

	}
}
