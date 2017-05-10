package nl.vu.cs.querypie.reasoner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.BlockFlow;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoning.expand.ExpandQuery;
import nl.vu.cs.querypie.reasoning.expand.QueryNode;
import nl.vu.cs.querypie.reasoning.expand.Queue;
import nl.vu.cs.querypie.reasoning.expand.RuleNode;
import nl.vu.cs.querypie.reasoning.expand.Tree;
import nl.vu.cs.querypie.reasoning.expand.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.utils.TripleBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimalBCAlgo extends Action {

	static final Logger log = LoggerFactory.getLogger(OptimalBCAlgo.class);

	public static final int L_FIELD1 = 0;
	public static final int L_FIELD2 = 1;
	public static final int L_FIELD3 = 2;
	public static final int B_EXPLICIT = 3;
	public static final int L_ITERATION = 4;
	public static final int B_FIRSTCALL = 5;

	private InMemoryTripleContainer outputContainer = null;
	private final RDFTerm[] triple = new RDFTerm[3];
	private ArrayList<Long> newTriplesIteration = new ArrayList<Long>();
	private boolean firstCall;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(L_FIELD1, "L_FIELD1", 0, true);
		conf.registerParameter(L_FIELD2, "L_FIELD2", 0, true);
		conf.registerParameter(L_FIELD3, "L_FIELD3", 0, true);
		conf.registerParameter(B_EXPLICIT, "B_EXPLICIT", false, false);
		conf.registerParameter(L_ITERATION, "L_ITERATION", 0, false);
		conf.registerParameter(B_FIRSTCALL, "B_FIRSTCALL", true, false);
	}

	public static void applyTo(RDFTerm v1, RDFTerm v2, RDFTerm v3,
			boolean excludeExplicit, ActionSequence actions)
					throws ActionNotConfiguredException {
		applyTo(v1, v2, v3, excludeExplicit,
				TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS, 0, actions);
	}

	public static void applyTo(RDFTerm v1, RDFTerm v2, RDFTerm v3,
			boolean explicit, int typeRules, long iteration,
			ActionSequence actions) throws ActionNotConfiguredException {

		ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
		c.setParamString(QueryInputLayer.S_INPUTLAYER,
				DummyLayer.class.getName());
		c.setParamWritable(
				QueryInputLayer.W_QUERY,
				new nl.vu.cs.ajira.actions.support.Query(TupleFactory.newTuple(
						v1, v2, v3, new TInt(0))));
		actions.add(c);

		c = ActionFactory.getActionConf(ExpandQuery.class);
		c.setParamBoolean(ExpandQuery.B_EXPLICIT, explicit);
		c.setParamInt(ExpandQuery.I_TYPE_RULES, typeRules);
		actions.add(c);

		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				RDFTerm.class.getName(), RDFTerm.class.getName(),
				RDFTerm.class.getName(), TBoolean.class.getName());
		c.setParamBoolean(CollectToNode.B_SORT, true);
		actions.add(c);

		actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));

		c = ActionFactory.getActionConf(OptimalBCAlgo.class);
		c.setParamLong(OptimalBCAlgo.L_FIELD1, v1.getValue());
		c.setParamLong(OptimalBCAlgo.L_FIELD2, v2.getValue());
		c.setParamLong(OptimalBCAlgo.L_FIELD3, v3.getValue());
		c.setParamBoolean(OptimalBCAlgo.B_EXPLICIT, explicit);
		c.setParamLong(OptimalBCAlgo.L_ITERATION, iteration);
		actions.add(c);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		Ruleset.getInstance().setQsqEvaluation(false);
		outputContainer = (InMemoryTripleContainer) context
				.getObjectFromCache("outputSoFar");
		if (outputContainer == null) {
			outputContainer = new InMemoryTripleContainer();
			context.putObjectInCache("outputSoFar", outputContainer);
		}
		newTriplesIteration.clear();
		firstCall = getParamBoolean(B_FIRSTCALL);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		triple[0] = (RDFTerm) inputTuple.get(0);
		triple[1] = (RDFTerm) inputTuple.get(1);
		triple[2] = (RDFTerm) inputTuple.get(2);
		if (!outputContainer.containsTriple(triple)) {
			actionOutput.output(inputTuple.get(0), inputTuple.get(1),
					inputTuple.get(2));
			if (outputContainer
					.addTriple(triple[0], triple[1], triple[2], null)) {
				newTriplesIteration.add(triple[0].getValue());
				newTriplesIteration.add(triple[1].getValue());
				newTriplesIteration.add(triple[2].getValue());
			}
		}
	}

	private static int getPosBindingsInRootQuery(QueryNode q, int pos)
			throws Exception {
		// Works only with rules type 2
		while (q.parent != null) {
			final RuleNode ruleNode = (RuleNode) q.parent;
			final Rule rule = ruleNode.rule;
			if (rule.type != 2) {
				throw new Exception("This should not happen");
			}
			final Rule2 rule2 = (Rule2) rule;
			final Mapping[] mappings = rule2.GENERICS_STRATS[0].pos_shared_vars_generics_head[0];
			boolean found = false;
			for (int i = 0; i < mappings.length; ++i) {
				if (mappings[i].pos1 == pos) {
					pos = mappings[i].pos2;
					found = true;
					break;
				}
			}
			if (!found) {
				return -1;
			}
			q = (QueryNode) q.parent.parent;
		}
		return pos;
	}

	protected static void addFilterQuery(QueryNode q, RuleNode childRule,
			InMemoryTripleContainer outputContainer, ActionContext context)
					throws Exception {
		// Can I filter the values?
		int nvars = 0;
		int lastpos = 0;
		if (q.getS() == Schema.ALL_RESOURCES) {
			nvars++;
			lastpos = 0;
		} else if (q.p == Schema.ALL_RESOURCES) {
			nvars++;
			lastpos = 1;
		} else if (q.o == Schema.ALL_RESOURCES) {
			nvars++;
			lastpos = 2;
		}
		if (nvars == 1) {
			final int pos = getPosBindingsInRootQuery(q, lastpos);
			if (pos >= 0 && outputContainer != null
					&& outputContainer.size() > 0) {
				// Get bindings and copy them in the rule execution
				final Set<Long> values = new HashSet<Long>();
				for (final nl.vu.cs.querypie.storage.memory.Triple t : outputContainer
						.getTripleSet()) {
					if (pos == 0) {
						values.add(t.subject);
					} else if (pos == 1) {
						values.add(t.predicate);
					} else {
						values.add(t.object);
					}
				}
				final int idFilterValues = context.getNewBucketID();
				context.putObjectInCache("filterValues-" + idFilterValues,
						values);
				context.broadcastCacheObjects("filterValues-" + idFilterValues);
				q.idFilterValues = idFilterValues;
				q.posFilterValues = lastpos;
				TreeExpander.filterRule(q, childRule);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected static boolean reduceSizeBindings(ArrayList<Long> newTriples,
			QueryNode node, ActionContext context) throws IOException {
		int nconstants = 0;
		int nsets = 0;
		int posSet = 0;
		long posKey = 0;
		if (node.getS() >= 0) {
			nconstants++;
		} else if (node.getS() <= RDFTerm.THRESHOLD_VARIABLE) {
			nsets++;
			posSet = 0;
			posKey = node.getS();
		}
		if (node.p >= 0) {
			nconstants++;
		} else if (node.p <= RDFTerm.THRESHOLD_VARIABLE) {
			nsets++;
			posSet = 1;
			posKey = node.p;
		}
		if (node.o >= 0) {
			nconstants++;
		} else if (node.o <= RDFTerm.THRESHOLD_VARIABLE) {
			nsets++;
			posSet = 2;
			posKey = node.o;
		}
		if (nconstants == 2 && nsets == 1) {
			// Get set
			Collection<Long> bindings = (Collection<Long>) context
					.getObjectFromCache(posKey);
			for (int i = posSet; i < newTriples.size(); i += 3) {
				bindings.remove(newTriples.get(i));
				if (bindings.size() == 0) {
					return true;
				}
			}
			context.broadcastCacheObjects(posKey);
		}
		return false;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {

		final Queue queue = (Queue) context.getObjectFromCache("queue");
		final Tree t = (Tree) context.getObjectFromCache("tree");

		if (firstCall || queue == null || queue.size() == 0) {
			/***
			 * First collect all intermediate triples that were being
			 * retrieved/inferred so far
			 ***/
			final List<TripleBuffer> triples = RuleBCAlgo
					.collectIntermediateData(context);
			if (triples != null && triples.size() > 0) {
				RuleBCAlgo.calculateIntermediateTriplesForNextRound(context,
						triples);
			}

			/***
			 * If a query contains a set of elements, reduce the sets of
			 * elements if new triples are being found. If the set is empty,
			 * than no more reasoning is necessary.
			 */
			if (reduceSizeBindings(newTriplesIteration, t.getQuery(0), context)) {
				return;
			}
		}

		/***
		 * If reasoning requires the applications of some rules of type 3/4,
		 * then I proceed executing them
		 ***/
		if (queue != null && queue.size() > 0) {
			// log.debug("Atoms to resolve: " + queue.size());
			// final Tree tree = (Tree) context.getObjectFromCache("tree");
			// System.out.println("Info tree:\n" + tree.info());

			QueryNode childQuery = queue.get();
			final RuleNode rule = (RuleNode) childQuery.parent;
			final QueryNode q = (QueryNode) rule.parent;
			childQuery.setQueued(false);

			/*** CREATE CHAIN ***/
			final ActionSequence actions = new ActionSequence();

			// Move to the next child
			if (!childQuery.wasUpdatedPreviousItr()) {
				childQuery = childQuery.next[0];
			}

			log.debug("Execute rule " + rule.rule.id + " with query "
					+ childQuery);

			// TupleSet cachedSet = null;
			ActionConf c;
			// if (rule.rule.type == 4)
			// cachedSet = t.isListAlreadyComputed(q, rule, childQuery);
			// if (cachedSet != null) {
			// rule.cachedTupleSet = cachedSet;
			// } else {
			// Result is not cached.
			c = ActionFactory.getActionConf(QueryInputLayer.class);
			c.setParamString(QueryInputLayer.S_INPUTLAYER,
					DummyLayer.class.getName());
			c.setParamWritable(
					QueryInputLayer.W_QUERY,
					new nl.vu.cs.ajira.actions.support.Query(TupleFactory
							.newTuple(new RDFTerm(childQuery.getS()),
									new RDFTerm(childQuery.p), new RDFTerm(
											childQuery.o),
											new TInt(childQuery.getId()))));
			actions.add(c);

			c = ActionFactory.getActionConf(ExpandQuery.class);
			c.setParamBoolean(ExpandQuery.B_EXPLICIT,
					getParamBoolean(B_EXPLICIT));
			c.setParamInt(ExpandQuery.I_TYPE_RULES,
					TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS);
			actions.add(c);

			// Collect to node
			c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName(), TBoolean.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);
			actions.add(c);

			// Remove duplicates
			actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));
			// }

			// Apply the rule
			ReasoningUtils.applyRule(actions, rule.rule,
					(QueryNode) rule.parent, childQuery, rule.strag_id,
					childQuery.current_pattern, "tree", childQuery.list_id,
					rule.idFilterValues,
					TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS);

			AddIntermediateTriples.applyTo(q.getS(), q.p, q.o, q.getId(),
					actions);

			actions.add(ActionFactory.getActionConf(BlockFlow.class));

			c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					RDFTerm.class.getName(), RDFTerm.class.getName(),
					RDFTerm.class.getName());
			c.setParamBoolean(CollectToNode.B_SORT, true);
			actions.add(c);

			c = ActionFactory.getActionConf(OptimalBCAlgo.class);
			c.setParamLong(OptimalBCAlgo.L_FIELD1, getParamLong(L_FIELD1));
			c.setParamLong(OptimalBCAlgo.L_FIELD2, getParamLong(L_FIELD2));
			c.setParamLong(OptimalBCAlgo.L_FIELD3, getParamLong(L_FIELD3));
			c.setParamBoolean(OptimalBCAlgo.B_EXPLICIT,
					getParamBoolean(B_EXPLICIT));
			c.setParamLong(OptimalBCAlgo.L_ITERATION, getParamLong(L_ITERATION));
			c.setParamBoolean(B_FIRSTCALL, false);
			actions.add(c);
			actionOutput.branch(actions);
		} else { // No more rules 3/4
			log.debug("New iteration");

			// t.resetListCache();

			/*** Should I repeat the process? ***/
			final long iteration = getParamLong(L_ITERATION);
			Long prevSize = (Long) context.getObjectFromCache("size-iter-"
					+ iteration);
			if (prevSize == null) {
				prevSize = 0l;
			}
			InMemoryTripleContainer indexedTriples = (InMemoryTripleContainer) context
					.getObjectFromCache("inputIntermediateTuples");
			final boolean shouldRepeat = indexedTriples != null
					&& indexedTriples.size() != prevSize;
			if (indexedTriples != null)
				context.putObjectInCache("size-iter-" + (iteration + 1),
						new Long(indexedTriples.size()));

			if (!shouldRepeat) {
				log.debug("Reasoning is finished");
				context.putObjectInCache("outputSoFar", null);
				outputContainer = null;
				return;
			}

			t.markAsUpdatedAllSimilarQueries(context);
			t.resetUpdateCounters();

			/*** Repeat the execution ***/
			final ActionSequence seq = new ActionSequence();
			OptimalBCAlgo.applyTo(new RDFTerm(getParamLong(L_FIELD1)),
					new RDFTerm(getParamLong(L_FIELD2)), new RDFTerm(
							getParamLong(L_FIELD3)),
							getParamBoolean(B_EXPLICIT),
							TreeExpander.ONLY_FIRST_SECOND_RECORD_OTHERS,
							getParamLong(L_ITERATION) + 1, seq);
			actionOutput.branch(seq);
		}
		outputContainer = null;
		newTriplesIteration.clear();
	}
}
