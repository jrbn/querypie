package nl.vu.cs.querypie.reasoner;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoning.expand.ExpandQuery;
import nl.vu.cs.querypie.reasoning.expand.ExpandTree;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

public class IncrRuleBCAlgo extends Action {

	public static final int L_NAMESET = 0;
	public static final int I_POSSET = 1;
	public static final int I_HEIGHT = 2;
	public static final int L_FIELD1 = 3;
	public static final int L_FIELD2 = 4;
	public static final int L_FIELD3 = 5;

	public static final void applyTo(RDFTerm v1, RDFTerm v2, RDFTerm v3,
			int posSet, long nameSet, ActionSequence actions)
			throws ActionNotConfiguredException {

		// Get the query
		ActionConf c = ActionFactory.getActionConf(QueryInputLayer.class);
		c.setParamString(QueryInputLayer.S_INPUTLAYER,
				DummyLayer.class.getName());
		c.setParamWritable(
				QueryInputLayer.W_QUERY,
				new nl.vu.cs.ajira.actions.support.Query(TupleFactory.newTuple(
						v1, v2, v3, new TInt(0))));
		actions.add(c);

		// Expand it in incremental fashion
		c = ActionFactory.getActionConf(ExpandQuery.class);
		c.setParamBoolean(ExpandQuery.B_EXPLICIT, true);
		c.setParamBoolean(ExpandQuery.B_ONLY_FIRST_AND_SECOND_RULES, true);
		c.setParamBoolean(ExpandQuery.B_ALLOWRECURSION, false);
		actions.add(c);

		// Collect to node
		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				RDFTerm.class.getName(), RDFTerm.class.getName(),
				RDFTerm.class.getName(), TBoolean.class.getName());
		actions.add(c);

		// Check whether we should repeat the process
		c = ActionFactory.getActionConf(IncrRuleBCAlgo.class);
		c.setParamLong(L_NAMESET, nameSet);
		c.setParamInt(I_POSSET, posSet);
		c.setParamLong(L_FIELD1, v1.getValue());
		c.setParamLong(L_FIELD2, v2.getValue());
		c.setParamLong(L_FIELD3, v3.getValue());
		actions.add(c);
	}

	private long nameSet;
	private HashSet<Long> toRemove;
	private int posSet;
	private int nextHeight;
	private Set<Long> set;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(L_NAMESET, "L_SET", null, true);
		conf.registerParameter(I_POSSET, "I_POSSET", null, true);
		conf.registerParameter(I_HEIGHT, "I_HEIGHT", 0, false);
		conf.registerParameter(L_FIELD1, "L_FIELD1", 0, true);
		conf.registerParameter(L_FIELD2, "L_FIELD2", 0, true);
		conf.registerParameter(L_FIELD3, "L_FIELD3", 0, true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		posSet = getParamInt(I_POSSET);
		nameSet = getParamLong(L_NAMESET);
		toRemove = new HashSet<Long>();
		nextHeight = getParamInt(I_HEIGHT);
		set = (Set<Long>) context.getObjectFromCache(nameSet);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		long v = ((RDFTerm) inputTuple.get(posSet)).getValue();
		if (!toRemove.contains(v)) {
			toRemove.add(v);
			actionOutput.output(inputTuple.get(0), inputTuple.get(1),
					inputTuple.get(2));
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		boolean finishedExpansion = context
				.getObjectFromCache(ExpandTree.FINISHED_EXPANSION) != null;

		set.removeAll(toRemove);
		toRemove = null;

		if (set.size() > 0) {
			ActionSequence actions = new ActionSequence();
			context.broadcastCacheObjects(nameSet);

			if (!finishedExpansion) {
				InMemoryTripleContainer[] intermediateTriples = RuleBCAlgo
						.collectIntermediateData(context);
				RuleBCAlgo.calculateIntermediateTriplesForNextRound(context,
						intermediateTriples[0], intermediateTriples[1]);

				// Read a tuple from the dummy layer
				ActionConf c = ActionFactory
						.getActionConf(QueryInputLayer.class);
				c.setParamString(QueryInputLayer.S_INPUTLAYER,
						DummyLayer.class.getName());
				c.setParamWritable(
						QueryInputLayer.W_QUERY,
						new nl.vu.cs.ajira.actions.support.Query(TupleFactory
								.newTuple()));
				actions.add(c);

				// Expand it in incremental fashion
				if (nextHeight == 0) {
					context.putObjectInCache(ExpandTree.FINISHED_EXPANSION,
							null);
					context.broadcastCacheObjects(ExpandTree.FINISHED_EXPANSION);
				}
				c = ActionFactory.getActionConf(ExpandTree.class);
				c.setParamInt(ExpandTree.I_HEIGHT, nextHeight + 2);
				actions.add(c);

				// Collect to node
				c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName(), TBoolean.class.getName());
				actions.add(c);

				// Check whether we should repeat the process
				c = ActionFactory.getActionConf(IncrRuleBCAlgo.class);
				c.setParamInt(I_POSSET, posSet);
				c.setParamLong(L_NAMESET, nameSet);
				c.setParamInt(I_HEIGHT, nextHeight + 2);
				c.setParamLong(L_FIELD1, getParamLong(L_FIELD1));
				c.setParamLong(L_FIELD2, getParamLong(L_FIELD2));
				c.setParamLong(L_FIELD3, getParamLong(L_FIELD3));
				actions.add(c);
			} else {
				InMemoryTripleContainer[] intermediateTriples = RuleBCAlgo
						.collectIntermediateData(context);
				InMemoryTripleContainer triples = intermediateTriples[0];
				InMemoryTripleContainer explicitTriples = intermediateTriples[1];

				if (triples != null && triples.size() > 0) {
					RuleBCAlgo.calculateIntermediateTriplesForNextRound(
							context, triples, explicitTriples);

					// Continue using the traditional ruleBasedAlgo
					RuleBCAlgo.applyTo(new RDFTerm(getParamLong(L_FIELD1)),
							new RDFTerm(getParamLong(L_FIELD3)), new RDFTerm(
									getParamLong(L_FIELD3)), false, actions);
				} else {
					return;
				}
			}
			actionOutput.branch(actions);
		}
	}
}
