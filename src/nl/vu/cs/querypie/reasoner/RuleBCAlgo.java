package nl.vu.cs.querypie.reasoner;

import java.io.IOException;
import java.util.List;

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
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;
import nl.vu.cs.querypie.storage.memory.Triple;
import nl.vu.cs.querypie.utils.TripleBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleBCAlgo extends Action {

	static final Logger log = LoggerFactory.getLogger(RuleBCAlgo.class);

	// Fields of the tuple parameter.
	public static final int L_FIELD1 = 0;
	public static final int L_FIELD2 = 1;
	public static final int L_FIELD3 = 2;
	public static final int B_EXPLICIT = 3;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(L_FIELD1, "L_FIELD1", 0, true);
		conf.registerParameter(L_FIELD2, "L_FIELD2", 0, true);
		conf.registerParameter(L_FIELD3, "L_FIELD3", 0, true);
		conf.registerParameter(B_EXPLICIT, "B_EXPLICIT", false, false);
	}

	private RDFTerm[] triple = new RDFTerm[3];
	private InMemoryTripleContainer outputContainer = null;
	private boolean explicit;

	public static void applyTo(RDFTerm v1, RDFTerm v2, RDFTerm v3,
			boolean explicit, ActionSequence actions)
					throws ActionNotConfiguredException {

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
		c.setParamBoolean(ExpandQuery.B_FORCERESTART, true);
		actions.add(c);

		c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				RDFTerm.class.getName(), RDFTerm.class.getName(),
				RDFTerm.class.getName(), TBoolean.class.getName());
		actions.add(c);

		// AddIntermediateTriples.applyTo(v1.getValue(), v2.getValue(),
		// v3.getValue(), actions);

		c = ActionFactory.getActionConf(RuleBCAlgo.class);
		c.setParamLong(L_FIELD1, v1.getValue());
		c.setParamLong(L_FIELD2, v2.getValue());
		c.setParamLong(L_FIELD3, v3.getValue());
		c.setParamBoolean(B_EXPLICIT, explicit);
		actions.add(c);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		explicit = getParamBoolean(B_EXPLICIT);
		outputContainer = (InMemoryTripleContainer) context
				.getObjectFromCache("outputSoFar");
		if (outputContainer == null) {
			outputContainer = new InMemoryTripleContainer();
			context.putObjectInCache("outputSoFar", outputContainer);
		}
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		triple[0] = (RDFTerm) inputTuple.get(0);
		triple[1] = (RDFTerm) inputTuple.get(1);
		triple[2] = (RDFTerm) inputTuple.get(2);
		if (!outputContainer.containsTriple(triple)) {
			output.output(inputTuple.get(0), inputTuple.get(1),
					inputTuple.get(2));
			outputContainer.addTriple(triple[0], triple[1], triple[2], null);
		}
	}

	@SuppressWarnings("unchecked")
	public static final List<TripleBuffer> collectIntermediateData(
			ActionContext context) {
		// Get local data structures
		List<TripleBuffer> triples = (List<TripleBuffer>) context
				.getObjectFromCache("intermediateTuples");
		// InMemoryTripleContainer explicitTriples = (InMemoryTripleContainer)
		// context
		// .getObjectFromCache("explicitIntermediateTuples");

		// Retrieve current inferred and explicit triples
		List<Object[]> remoteObjs = context
				.retrieveCacheObjects("intermediateTuples"/*
				 * ,
				 * "explicitIntermediateTuples"
				 */);
		if (remoteObjs != null) {
			for (Object[] values : remoteObjs) {
				// Inferred
				List<TripleBuffer> v = (List<TripleBuffer>) values[0];
				if (v != null && v.size() > 0) {
					if (triples == null) {
						triples = v;
					} else {
						triples.addAll(v);
					}
				}
				//
				// // Explicit
				// v = (InMemoryTripleContainer) values[1];
				// if (v != null && v.size() > 0) {
				// if (explicitTriples == null) {
				// explicitTriples = v;
				// } else {
				// explicitTriples.addAll(v);
				// }
				// }
			}
		}

		// // Remove eventual inferred triples that are explicit in DB
		// if (explicitTriples != null && triples != null) {
		// triples.removeAll(explicitTriples);
		// }

		// InMemoryTripleContainer[] output = new InMemoryTripleContainer[2];
		// output[0] = triples;
		// output[1] = explicitTriples;
		return triples;
	}

	public static final void calculateIntermediateTriplesForNextRound(
			ActionContext context, List<TripleBuffer> triples) throws Exception {

		InMemoryTripleContainer previousTriples = (InMemoryTripleContainer) context
				.getObjectFromCache("inputIntermediateTuples");
		if (previousTriples != null) {
			if (triples != null) {

				for (TripleBuffer b : triples) {
					int size = b.getNElements();
					for (int i = 0; i < size; ++i) {
						long[] rawTriple = b.get(i);
						Triple triple = new Triple();
						triple.subject = rawTriple[0];
						triple.predicate = rawTriple[1];
						triple.object = rawTriple[2];
						previousTriples.addIndexedTriple(triple);
					}
				}
			}
		} else if (triples != null) {
			InMemoryTripleContainer output = new InMemoryTripleContainer();
			for (TripleBuffer b : triples) {
				int size = b.getNElements();
				for (int i = 0; i < size; ++i) {
					long[] rawTriple = b.get(i);
					Triple triple = new Triple();
					triple.subject = rawTriple[0];
					triple.predicate = rawTriple[1];
					triple.object = rawTriple[2];
					output.addTriple(triple, null);
				}
			}
			output.index();
			context.putObjectInCache("inputIntermediateTuples", output);
		}
		context.putObjectInCache("intermediateTuples", null);
		context.broadcastCacheObjects("inputIntermediateTuples",
				"intermediateTuples");
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		List<TripleBuffer> triples = collectIntermediateData(context);
		// InMemoryTripleContainer triples = collectedTriples[0];
		// InMemoryTripleContainer explicitTriples = collectedTriples[1];

		if (triples != null)
			context.incrCounter("total results", triples.size());

		// Check whether I need to relaunch the query
		if (triples != null && triples.size() > 0) {
			calculateIntermediateTriplesForNextRound(context, triples);

			/***** Repeat reasoning *****/
			ActionSequence seq = new ActionSequence();
			RuleBCAlgo.applyTo(new RDFTerm(getParamLong(L_FIELD1)),
					new RDFTerm(getParamLong(L_FIELD2)), new RDFTerm(
							getParamLong(L_FIELD3)), explicit, seq);
			output.branch(seq);
		} else {
			// if (explicitTriples != null)
			// context.incrCounter("total results", explicitTriples.size());
		}
		outputContainer = null;
	}

	public static void cleanup(ActionContext context) throws IOException {
		context.putObjectInCache("tree", null);
		context.putObjectInCache("queue", null);
		context.putObjectInCache("intermediateTuples", null);
		context.putObjectInCache("inputIntermediateTuples", null);
		context.putObjectInCache("outputSoFar", null);
		context.putObjectInCache("explicitIntermediateTuples", null);
		context.broadcastCacheObjects("tree", "intermediateTuples",
				"inputIntermediateTuples", "explicitIntermediateTuples",
				"outputSoFar");
	}
}
