package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;
import java.util.List;

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
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QSQEvaluateQuery extends Action {

	protected static final Logger log = LoggerFactory
			.getLogger(QSQEvaluateQuery.class);

	public static final int I_QUERY_ID = 0;
	public static final int B_FILTER = 1;

	private int id;
	private InMemoryTripleContainer outputContainer;
	private InMemoryTripleContainer existingDerivation;
	private long previousSize;
	private boolean filter;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_QUERY_ID, "I_QUERY_ID", -1, true);
		conf.registerParameter(B_FILTER, "B_FILTER", false, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		id = getParamInt(I_QUERY_ID);
		filter = getParamBoolean(B_FILTER);
		outputContainer = new InMemoryTripleContainer();
		previousSize = 0;
		existingDerivation = (InMemoryTripleContainer) context
				.getObjectFromCache("inputIntermediateTuples");
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		boolean derived = ((TBoolean) tuple.get(3)).getValue();
		if ((!derived && !filter)
				|| outputContainer.addTriple((RDFTerm) tuple.get(0),
						(RDFTerm) tuple.get(1), (RDFTerm) tuple.get(2),
						existingDerivation)) {
			actionOutput.output(tuple);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// Should I repeat the process? If query id is 0 (root), check also that
		// no more triple has been derived
		final Tree t = (Tree) context.getObjectFromCache(QSQBCAlgo.TREE_ID);
		QueryNode query = t.getQuery(id);

		boolean shouldRepeat = outputContainer != null
				&& outputContainer.size() != previousSize;
		if (query.getId() == 0) {
			// Check also that no intermediate derivations have been produced
			if (existingDerivation != null) {
				if (!shouldRepeat) {
					Long previousSize = (Long) context
							.getObjectFromCache("previousDerivation");
					if (previousSize == null) {
						previousSize = new Long(0);
					}
					shouldRepeat = existingDerivation.size() != previousSize;
				}
			}
		}

		if (shouldRepeat) {
			// Index the intermediate data
			// Copy the data to the intermediate container used by RDFStorage
			t.markAsUpdatedAllSimilarQueries(context);
			t.resetUpdateCounters();
			if (outputContainer.size() > 0) {
				if (existingDerivation == null) {
					existingDerivation = outputContainer;
					existingDerivation.index();
					context.putObjectInCache("inputIntermediateTuples",
							existingDerivation);
				} else {
					existingDerivation.addAllIndexed(outputContainer);
				}
			}

			if (query.getId() == 0)
				context.putObjectInCache("previousDerivation", new Long(
						existingDerivation.size()));

			final ActionSequence actions = new ActionSequence();
			if (query.getId() == 0) {
				// cleanupCache(context); //Conflicts with SPARQL
				evaluateRootQuery(actions, query.getS(), query.p, query.o,
						context);
			} else {
				evaluateQuery(actions, t, query, false, context);
			}
			actionOutput.branch(actions);
		} else {
			query.setComputed();
			log.debug("Finished computing query " + query);
		}

		outputContainer = null;
		existingDerivation = null;
	}

	public static final void cleanupCache(ActionContext context) {
		// Cleanup the cache
		List<Long> toRemove = new ArrayList<>();
		for (Object obj : context.getAllObjectsFromCache()) {
			if (obj instanceof Long) {
				Long l = (Long) obj;
				if (l < RDFTerm.THRESHOLD_VARIABLE - 100) { // -100 is an hack
					// to make sure that
					// the SPARQL
					// variables are not
					// being cleaned up.
					toRemove.add(l);
				}
			}
		}
		for (Long el : toRemove) {
			context.removeFromCache(el);
		}
	}

	public static final void evaluateRootQuery(ActionSequence actions, long v1,
			long v2, long v3, ActionContext context) throws Exception {

		final Tree t = new Tree();
		final QueryNode q = t.newQuery(null);
		q.setS(v1);
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
			TreeExpander.expandQuery(context, q, t, TreeExpander.ALL, null);
			q.setExpanded();
		}

		if (explicit) {
			if (q.getS() == Schema.SCHEMA_SUBSET) {
				final ActionConf c = ActionFactory
						.getActionConf(QueryInputLayer.class);
				c.setParamString(QueryInputLayer.S_INPUTLAYER,
						DummyLayer.class.getName());
				c.setParamWritable(
						QueryInputLayer.W_QUERY,
						new nl.vu.cs.ajira.actions.support.Query(TupleFactory
								.newTuple(new RDFTerm(q.getS()), new RDFTerm(
										q.p), new RDFTerm(q.o),
										new TInt(q.getId()))));
				actions.add(c);
			} else { // Read from the default layer
				ReasoningUtils.getResultsQuery(actions, TupleFactory.newTuple(
						new RDFTerm(q.getS()), new RDFTerm(q.p), new RDFTerm(
								q.o)), false);
				actions.add(ActionFactory.getActionConf(SetAsExplicit.class));
			}
		} else {
			final QueryNode child = (QueryNode) q.child.child;
			applyRule(actions, t, q, (RuleNode) q.child, child, child.list_id,
					((RuleNode) q.child).idFilterValues, context);
		}

		final boolean shouldRepeat = !q.isComputed(context) && q.child != null;
		if (shouldRepeat) {
			// Add the move to sibling action (if there are siblings)
			if (explicit) {
				ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName(), TBoolean.class.getName());
				// c.setParamBoolean(CollectToNode.B_SORT, true);
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
				// c.setParamBoolean(CollectToNode.B_SORT, true);
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
			c.setParamBoolean(CollectToNode.B_SORT, true);
			actions.add(c);

			// Repeat the process adding QSQAlgo
			c = ActionFactory.getActionConf(QSQEvaluateQuery.class);
			c.setParamInt(I_QUERY_ID, q.getId());
			if (q.getId() == 0)
				c.setParamBoolean(B_FILTER, true);
			actions.add(c);
		} else {
			// Mark it as computed
			if (!q.isComputed(context)) {
				q.setComputed();
			}

			// This query was either being computed or no reasoning could be
			// applied. In the second case,
			// A query to the datalayer might return some duplicates that we
			// need to remove.
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
			QueryNode head, RuleNode rule, QueryNode query, int list_id,
			int idFilterValues, ActionContext context) throws Exception {
		applyRule(newChain, tree, head, rule.rule, rule.single_node,
				rule.strag_id, query.current_pattern, query, list_id,
				idFilterValues, context);
	}

	public static final void applyRule(ActionSequence newChain, Tree tree,
			QueryNode head, Rule rule, boolean single_node, int strag_id,
			int current_pattern, QueryNode query, int list_id,
			int idFilterValues, ActionContext context) throws Exception {

		evaluateQuery(newChain, tree, query, true, context);

		if (query.sibling != null) {
			ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);
			newChain.add(c);

			c = ActionFactory.getActionConf(QSQMoveNextSibling.class);
			c.setParamInt(QSQMoveNextSibling.I_NODE_ID, query.sibling.getId());
			c.setParamBoolean(QSQMoveNextSibling.B_RULE, false);
			newChain.add(c);
		}

		if (single_node) {
			final ActionConf c = ActionFactory
					.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);
			newChain.add(c);
		}

		ReasoningUtils.applyRule(newChain, rule, head, query, strag_id,
				current_pattern, QSQBCAlgo.TREE_ID, list_id, idFilterValues,
				TreeExpander.ALL);

	}
}
