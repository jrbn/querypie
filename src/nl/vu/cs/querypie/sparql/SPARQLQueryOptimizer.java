package nl.vu.cs.querypie.sparql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoning.expand.QueryNode;
import nl.vu.cs.querypie.reasoning.expand.Tree;
import nl.vu.cs.querypie.reasoning.expand.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPARQLQueryOptimizer extends Action {

	public static final int I_MAX_LEVELS = 0;
	public static final int B_INCLUDE_IMPLICIT = 1;

	public static final class Query {
		long[] names = new long[3];
		long[] values = new long[3];
		int nconstants = 0;
		public long explicitEstimate = 0;
		public long implicitEstimate = 0;

		public int njoins(Set<Long> vars) {
			int n = 0;
			for (int i = 0; i < 3; ++i) {
				if (names[i] < 0 && vars.contains(names[i])) {
					n++;
				}
			}
			return n;
		}

		public List<Long> getVars() {
			List<Long> vars = new ArrayList<Long>();
			for (int i = 0; i < 3; ++i) {
				if (names[i] < 0) {
					vars.add(names[i]);
				}
			}
			return vars;
		}
	}

	static final Logger log = LoggerFactory
			.getLogger(SPARQLQueryOptimizer.class);

	private int maxLevels;
	private boolean includeImplicit;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_MAX_LEVELS, "I_MAX_LEVELS", null, true);
		conf.registerParameter(B_INCLUDE_IMPLICIT, "B_INCLUDE_IMPLICIT", true,
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		maxLevels = getParamInt(I_MAX_LEVELS);
		includeImplicit = getParamBoolean(B_INCLUDE_IMPLICIT);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		try {
			long[] serializedJoin = new long[inputTuple.getNElements()];
			// Fill it from the tuple
			for (int i = 0; i < inputTuple.getNElements(); ++i) {
				RDFTerm number = (RDFTerm) inputTuple.get(i);
				serializedJoin[i] = number.getValue();
			}
			if (log.isDebugEnabled()) {
				log.debug("serializedJoin: " + Arrays.toString(serializedJoin));
			}

			optimizeQuery(serializedJoin, output, context);

			if (log.isDebugEnabled()) {
				log.debug("Optimized serializedJoin: "
						+ Arrays.toString(serializedJoin));
			}

			RDFTerm[] tuple = new RDFTerm[serializedJoin.length];
			for (int i = 0; i < serializedJoin.length; ++i) {
				tuple[i] = new RDFTerm(serializedJoin[i]);
			}
			output.output(tuple);

		} catch (Exception e) {
			log.error("Error processing query", e);
		}

	}

	private ArrayList<Query> parseQueries(long[] query) {
		ArrayList<Query> list = new ArrayList<Query>();
		for (int i = 0; i < query.length; i += 3) {
			Query q = new Query();
			for (int j = 0; j < 3; ++j) {
				q.names[j] = query[i + j];
				if (q.names[j] >= 0) {
					q.nconstants++;
					q.values[j] = q.names[j];
				} else {
					q.values[j] = -1;
				}
			}
			list.add(q);
		}
		return list;
	}

	private static final void sortQueriesByCardinalityAndNConstants(
			ArrayList<Query> queries) {
		if (log.isDebugEnabled()) {
			for (Query q : queries) {
				log.debug("Query: " + q.names[0] + " " + q.names[1] + " "
						+ q.names[2]);
				log.debug("explicit: " + q.explicitEstimate + ", implicit: "
						+ q.implicitEstimate);
			}
		}
		// Sort them by number of constants
		Collections.sort(queries, new Comparator<Query>() {
			@Override
			public int compare(Query o1, Query o2) {
				long totalEstimate2 = Math.min(o2.explicitEstimate
						+ o2.implicitEstimate, Integer.MAX_VALUE);
				long totalEstimate1 = Math.min(o1.explicitEstimate
						+ o1.implicitEstimate, Integer.MAX_VALUE);
				long output = totalEstimate1 - totalEstimate2;
				if (output == 0) {
					return o2.nconstants - o1.nconstants;
				} else if (output < 0) {
					return -1;
				} else {
					return 1;
				}
			}
		});
	}

	private static final long[] estimateCardinality(List<long[]> queries,
			ActionOutput actionOutput, ActionContext context) throws Exception {
		final nl.vu.cs.ajira.utils.Lock lock = new nl.vu.cs.ajira.utils.Lock();
		context.putObjectInCache(EstimateCardinality.LOCK, lock);

		for (int i = 0; i < queries.size(); ++i) {
			long[] q = queries.get(i);
			if (log.isDebugEnabled()) {
				log.debug("estimateCardinality: [" + Arrays.toString(q) + "]");
			}
			ActionSequence actions = new ActionSequence();

			ReasoningUtils.getResultsQuery(actions, TupleFactory.newTuple(
					new RDFTerm(q[0]), new RDFTerm(q[1]), new RDFTerm(q[2])),
					false);

			ActionConf c = ActionFactory
					.getActionConf(EstimateCardinality.class);
			c.setParamInt(EstimateCardinality.I_IDQUERY, i);
			actions.add(c);

			c = ActionFactory.getActionConf(CollectToNode.class);
			c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
					TInt.class.getName(), TLong.class.getName());
			actions.add(c);

			c = ActionFactory.getActionConf(EstimateCardinality.class);
			c.setParamInt(EstimateCardinality.I_IDQUERY, i);
			c.setParamBoolean(EstimateCardinality.B_SUMVALUES, true);
			actions.add(c);

			actionOutput.branch(actions);
		}

		// Wait until all estimations are completed
		synchronized (lock) {
			if (lock.getCount() != queries.size()) {
				context.startBlocking();
			}
			while (lock.getCount() != queries.size()) {
				lock.wait();
			}
		}

		// Now in the cache there are estimation for all the queries...
		long[] output = new long[queries.size()];
		for (int i = 0; i < queries.size(); ++i) {
			output[i] = (Long) context.getObjectFromCache("estimate-" + i);
		}
		return output;
	}

	public static final void estimateImplicitCardinality(
			ArrayList<Query> queries, int maxLevels, ActionOutput actionOutput,
			ActionContext context) throws Exception {
		final nl.vu.cs.ajira.utils.Lock lock = new nl.vu.cs.ajira.utils.Lock();
		context.putObjectInCache(EstimateCardinality.LOCK, lock);

		for (int i = 0; i < queries.size(); ++i) {
			Query q = queries.get(i);

			Tree tree = new Tree();
			QueryNode query = tree.newQuery(null);
			query.setS(q.values[0]);
			query.p = q.values[1];
			query.o = q.values[2];

			int currentLevel = 0;
			while (currentLevel < maxLevels) {
				for (int j = tree.getNQueries() - 1; j >= 0; j--) {
					QueryNode queryTree = tree.getQuery(j);
					if (queryTree.height == currentLevel * 2) {
						TreeExpander.expandQuery(context, queryTree, tree,
								TreeExpander.ALL, null);
					} else {
						// Since the tree is created breadth-first, I can stop
						// when I finished the level
						break;
					}
				}
				currentLevel++;
			}

			if (log.isDebugEnabled())
				log.debug("Estimating the cardinality of pattern "
						+ query.getS() + " " + query.p + " " + query.o
						+ " has generated " + (tree.getNQueries() - 1)
						+ " queries");

			// Estimate the cardinality of these queries
			List<long[]> queriesToEstimate = new ArrayList<long[]>();
			for (int j = 1; j < tree.getNQueries(); ++j) {
				QueryNode queryTree = tree.getQuery(j);
				long[] queryToEstimate = new long[3];
				queryToEstimate[0] = queryTree.getTerm(0);
				queryToEstimate[1] = queryTree.getTerm(1);
				queryToEstimate[2] = queryTree.getTerm(2);
				queriesToEstimate.add(queryToEstimate);
			}

			q.implicitEstimate = 0;
			if (queriesToEstimate.size() > 0) {
				long[] estimates = estimateCardinality(queriesToEstimate,
						actionOutput, context);
				for (long estimate : estimates) {
					q.implicitEstimate += estimate;
				}
			}
		}
	}

	public static final void estimateExplicitCardinality(
			ArrayList<Query> queries, ActionOutput actionOutput,
			ActionContext context) throws Exception {
		final nl.vu.cs.ajira.utils.Lock lock = new nl.vu.cs.ajira.utils.Lock();
		context.putObjectInCache(EstimateCardinality.LOCK, lock);

		List<long[]> rawQueries = new ArrayList<long[]>();
		for (int i = 0; i < queries.size(); ++i) {
			Query q = queries.get(i);
			long[] query = Arrays.copyOf(q.values, 3);
			rawQueries.add(query);
		}

		long[] card = estimateCardinality(rawQueries, actionOutput, context);

		// Now in the cache there are estimation for all the queries...
		for (int i = 0; i < card.length; ++i) {
			Query q = queries.get(i);
			q.explicitEstimate = card[i];
		}

	}

	public static final void rearrangeQueriesByNumberJoins(
			ArrayList<Query> queries) throws Exception {
		// Pick first query
		Set<Long> vars = new HashSet<Long>();
		for (int i = 0; i < queries.size() - 1; ++i) {
			Query query = queries.get(i);
			vars.addAll(query.getVars());
			int idxNextQuery = -1;
			int njoinsNextQuery = 0;
			for (int j = i + 1; j < queries.size(); ++j) {
				Query possibleNextQuery = queries.get(j); // Join variables?
				int njoinvars = possibleNextQuery.njoins(vars);
				if (njoinvars > njoinsNextQuery) {
					idxNextQuery = j;
					njoinsNextQuery = njoinvars;
				}
			}
			if (idxNextQuery == -1) {
				throw new Exception(
						"This pattern does not join with any other ones");
			} else {
				Query nextQuery = queries.get(idxNextQuery);
				for (int m = idxNextQuery; m > i + 1; m--) {
					queries.set(m, queries.get(m - 1));
				}
				queries.set(i + 1, nextQuery);
			}
		}

	}

	private void optimizeQuery(long[] query, ActionOutput output,
			ActionContext context) throws Exception {

		// First I sort the queries according to the number of constants.
		ArrayList<Query> queries = parseQueries(query);

		if (queries.size() <= 1) {
			return;
		}

		// Estimate the cardinality of the queries
		estimateExplicitCardinality(queries, output, context);

		// Rearrange them considering the cardinality with reasoning
		if (includeImplicit) {
			estimateImplicitCardinality(queries, maxLevels, output, context);
		}

		// Sort them by cardinality and nconstants
		sortQueriesByCardinalityAndNConstants(queries);

		// Construct the order making sure that there is always one or more
		// variable to join
		rearrangeQueriesByNumberJoins(queries);

		// Serialize it into an array
		int i = 0;
		for (Query q : queries) {
			query[i++] = q.names[0];
			query[i++] = q.names[1];
			query[i++] = q.names[2];
		}
	}
}
