package nl.vu.cs.querypie.reasoner.rules.executors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2.GenericVars;
import nl.vu.cs.querypie.reasoner.rules.Rule3;
import nl.vu.cs.querypie.reasoning.expand.QSQEvaluateQuery;
import nl.vu.cs.querypie.reasoning.expand.QueryNode;
import nl.vu.cs.querypie.reasoning.expand.Tree;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.CacheCollectionValues;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.MultiValue;
import nl.vu.cs.querypie.storage.memory.TupleMap;
import nl.vu.cs.querypie.storage.memory.TupleSet;
import nl.vu.cs.querypie.storage.memory.tuplewrapper.ArrayTupleWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleExecutor3 extends RuleExecutor2 {

	static final Logger log = LoggerFactory.getLogger(RuleExecutor3.class);

	public static final int I_STRAG_ID = 4;
	public static final int I_PATTERN_POS = 5;
	public static final int I_KEY = 6;
	public static final int I_QUERY_ID = 7;

	@Override
	public void registerActionParameters(ActionConf conf) {
		super.registerActionParameters(conf);
		conf.registerParameter(I_STRAG_ID, "I_STRAG_ID", 0, true);
		conf.registerParameter(I_PATTERN_POS, "I_PATTERN_POS", 0, true);
		conf.registerParameter(I_KEY, "I_KEY", 0, true);
		conf.registerParameter(I_QUERY_ID, "I_QUERY_ID", 0, true);
	}

	protected RDFTerm term = new RDFTerm();
	protected TupleSet newTupleSet;

	private Rule3 ruleDef;

	protected TupleMap mapping_generic_actual_tuples;
	protected MultiValue mkey_mapping_actual_tuples_generic = new MultiValue();
	protected long[] key_mapping_actual_tuples_generic;

	protected int strag_id;
	protected TupleSet actual_precomputed_tuples;

	// The current generic pattern
	protected int current_generic_pattern_pos;
	// Is the last pattern to process?
	private boolean last_generic_pattern;

	// If the generic pattern currently retrieved is the same as the next one,
	// then do not fetch the next
	protected boolean fetch_next_pattern;
	private boolean do_in_memory_join;
	private List<long[]> second_generic_pattern;

	protected void parse_current_state(Rule1 rule, GenericVars g,
			boolean force_store_in_memory, boolean join_in_memory,
			ActionContext context) {
		int pattern_pos = getParamInt(I_PATTERN_POS);
		Object key = getParamInt(I_KEY);
		parse_current_state(rule, g, g.patterns[pattern_pos], pattern_pos, key,
				force_store_in_memory, false, context);
	}

	protected void parse_current_state(GenericVars g, ActionContext context) {
		int pattern_pos = getParamInt(I_PATTERN_POS);
		Object key = getParamInt(I_KEY);
		parse_current_state(ruleDef, g, g.patterns[pattern_pos], pattern_pos,
				key, false, true, context);
	}

	protected void parse_current_state(Rule1 ruleDef, GenericVars g,
			Pattern pattern, int pattern_pos, Object key,
			boolean force_store_in_memory, boolean join_in_memory,
			ActionContext context) {
		this.g = g;
		current_generic_pattern_pos = pattern_pos;
		last_generic_pattern = !(pattern_pos < (g.patterns.length - 1));
		fetch_next_pattern = !last_generic_pattern;

		if (pattern_pos != 0) {
			actual_precomputed_tuples = (TupleSet) context
					.getObjectFromCache(key);
		} else {
			actual_precomputed_tuples = ruleDef.precomputed_tuples;
		}

		if (!last_generic_pattern || force_store_in_memory) {

			// Not the last generic pattern. Save the variables in memory.
			int[] input = new int[g.pos_shared_vars_precomp_generics[pattern_pos].length];
			for (int i = 0; i < input.length; ++i)
				input[i] = g.pos_shared_vars_precomp_generics[pattern_pos][i].pos1;

			if (actual_precomputed_tuples == ruleDef.precomputed_tuples)
				mapping_generic_actual_tuples = actual_precomputed_tuples
						.getBindingsFromBindings(input, null, true);
			else
				mapping_generic_actual_tuples = actual_precomputed_tuples
						.getBindingsFromBindings(input, null);

			// Set the datastructure for the lookup
			mkey_mapping_actual_tuples_generic
					.setNumberFields(g.pos_shared_vars_precomp_generics[pattern_pos].length);
			key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
					.getRawFields();

			newTupleSet = new TupleSet(TupleSet.concatenateBindings(
					actual_precomputed_tuples.getNameBindings(), pattern));

			// Check if the next pattern is the same as this one. If yes, we do
			// the join immediately
			do_in_memory_join = g.are_generics_equivalent && join_in_memory;
			if (do_in_memory_join) {
				for (Mapping map : g.pos_shared_vars_generics_head[0]) {
					// Must be unbounded
					if (instantiated_head[map.pos2].getValue() != -1) {
						do_in_memory_join = false;
					}
				}
				if (do_in_memory_join) {
					for (Mapping map : g.pos_shared_vars_generics_head[1]) {
						// Must be unbounded
						if (instantiated_head[map.pos2].getValue() != -1) {
							do_in_memory_join = false;
						}
					}
				}

				if (do_in_memory_join) {
					fetch_next_pattern = false;
					second_generic_pattern = new ArrayList<long[]>();
				}
			}
		}
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		ruleDef = ruleset.getRuleThirdType(getParamInt(I_RULEDEF));

		// Instantiate the output
		for (int i = 0; i < 3; ++i) {
			instantiated_head[i].setValue(getParamLong(i));
			triple[i].setValue(ruleDef.HEAD.p[i].getValue());
		}

		strag_id = getParamInt(I_STRAG_ID);
		g = ruleDef.GENERICS_STRATS[strag_id];
		parse_current_state(g, context);

		if (last_generic_pattern && !do_in_memory_join) {
			prepareForJoin(ruleDef, actual_precomputed_tuples, g,
					instantiated_head, false, context);
		}

		// context.incrCounter("rule-" + ruleDef.type + "-" + ruleDef.id, 1);
	}

	protected void processGenericsIntermediate(Tuple inputTuple)
			throws Exception {
		RDFTerm term;
		// Extract the fields of the tuple to join the precomputed pattern
		for (int i = 0; i < g.pos_shared_vars_precomp_generics[current_generic_pattern_pos].length; ++i) {
			term = (RDFTerm) inputTuple
					.get(g.pos_shared_vars_precomp_generics[current_generic_pattern_pos][i].pos2);
			key_mapping_actual_tuples_generic[i] = term.getValue();
		}

		CollectionTuples match = mapping_generic_actual_tuples
				.get(mkey_mapping_actual_tuples_generic);
		if (match != null) {
			long[] list_variable_values = new long[g.pos_unique_vars_generic_patterns[current_generic_pattern_pos].length];
			for (int i = 0; i < g.pos_unique_vars_generic_patterns[current_generic_pattern_pos].length; ++i) {
				term = (RDFTerm) inputTuple
						.get(g.pos_unique_vars_generic_patterns[current_generic_pattern_pos][i]);
				list_variable_values[i] = term.getValue();
			}

			int sizeTuple = match.getSizeTuple();
			long[] rawValues = match.getRawValues();
			for (int y = match.getStart(); y < match.getEnd(); y += sizeTuple) {
				long[] concat = TupleSet.concatenateTuples(rawValues, y,
						sizeTuple, list_variable_values);
				newTupleSet.addTuple(concat);

				if (do_in_memory_join) {
					long[] triple = new long[3];
					for (int i = 0; i < triple.length; ++i) {
						triple[i] = ((RDFTerm) inputTuple.get(i)).getValue();
					}
					second_generic_pattern.add(triple);
				}
			}
		}
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (!last_generic_pattern) { // Execution rules types 2 and 3.
			processGenericsIntermediate(inputTuple);
		} else {
			tw.tuple = inputTuple;
			duplicates.clear();
			doJoin(tw, output, duplicates);
			duplicates.clear();
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {

		if (!fetch_next_pattern) {
			if (do_in_memory_join) {
				Set<MultiValue> existingValues = new HashSet<MultiValue>();
				for (long[] triple : second_generic_pattern) {
					MultiValue v = new MultiValue(triple);
					existingValues.add(v);
				}

				prepareForJoin(ruleDef, newTupleSet, g, instantiated_head,
						false, context);
				ArrayTupleWrapper atw = new ArrayTupleWrapper();
				for (long[] t : second_generic_pattern) {
					atw.values = t;
					doJoin(atw, output, existingValues);
				}
			}

		} else {
			generate_chain_for_next_step(ruleDef, context, output);
		}

		second_generic_pattern = null;
		actual_precomputed_tuples = null;
		mapping_generic_actual_tuples = null;
		key_mapping_actual_tuples_generic = null;
		newTupleSet = null;

		super.stopProcess(context, output);
	}

	private void unify(RDFTerm[] triple, RDFTerm[] instantiated_head,
			Pattern pattern, Mapping[] pos_variables_head, Mapping map,
			long shared_set_id) {
		// Unify tripleOutput with the values of the GENERIC PATTERN
		for (int i = 0; i < 3; ++i) {
			triple[i].setValue(pattern.p[i].getValue());
		}

		// Unify the triple with the values from the head that are not
		// used anywhere else
		if (pos_variables_head != null)
			for (Mapping pos : pos_variables_head) {
				triple[pos.pos1].setValue(instantiated_head[pos.pos2]
						.getValue());
			}

		// Unify the triple adding a reference to the set just
		// calculated
		triple[map.pos2].setValue(shared_set_id);
	}

	protected void generate_chain_for_next_step(Rule rule,
			ActionContext context, ActionOutput output) throws Exception {
		generate_chain_for_next_step(
				rule,
				g.patterns[current_generic_pattern_pos + 1],
				g.shared_var_with_next_gen_pattern[current_generic_pattern_pos],
				g.pos_shared_vars_generics_head[current_generic_pattern_pos + 1],
				current_generic_pattern_pos + 1, strag_id, -1, -1, context,
				output);

	}

	protected void generate_chain_for_next_step(Rule rule,
			Pattern next_pattern, Mapping var_shared_with_next_pattern,
			Mapping[] var_shared_generics, int next_pos, int strag_id,
			long list_head, int list_id, ActionContext context,
			ActionOutput output) throws Exception {

		if (newTupleSet.size() > 0) {
			// Generate the list of variable bindings
			int id = context.getNewBucketID();
			context.putObjectInCache(id, newTupleSet);
			Collection<Long> values = newTupleSet.getAllValues(
					var_shared_with_next_pattern.nameBinding, false);

			long shared_set_id;
			if (values.size() > 1) {

				CacheCollectionValues cache = getCache(context);
				CacheCollectionValues.Entry entry = cache.getCollection(values);
				if (entry == null) {
					shared_set_id = ((long) (context.getNewBucketID() * -1)) << 16;
					context.putObjectInCache(shared_set_id, values);
					context.broadcastCacheObjects(shared_set_id);
					cache.putCollection(shared_set_id, values);
					context.incrCounter("cache miss", 1);
				} else {
					context.incrCounter("cache hits", 1);
					shared_set_id = entry.id;
				}

			} else {
				shared_set_id = values.iterator().next();
			}

			unify(triple, instantiated_head, next_pattern, var_shared_generics,
					var_shared_with_next_pattern, shared_set_id);

			// Set the variable shared with the previous pattern
			Mapping[] shared_vars = newTupleSet
					.calculateSharedVariables(next_pattern);
			Collection<Long> col = null;
			Mapping m = null;
			for (Mapping map : shared_vars) {
				if (map.pos2 != var_shared_with_next_pattern.pos2) {
					col = newTupleSet.getAllValues(map.nameBinding, false);
					m = map;
				}
			}
			if (col != null) {
				Tree t = (Tree) context.getObjectFromCache("tree");
				QueryNode q = t.getQuery(getParamInt(RuleExecutor3.I_QUERY_ID));

				for (long val : col) {
					triple[m.pos2].setValue(val);

					QueryNode newQuery = t.newQuery(q.parent);
					newQuery.s = triple[0].getValue();
					newQuery.p = triple[1].getValue();
					newQuery.o = triple[2].getValue();
					newQuery.sibling = q.sibling;
					q.sibling = newQuery;
					q = newQuery;

					if (!Ruleset.getInstance().getQSQEvaluation()) {
						ReasoningUtils.generate_new_chain(output, rule,
								strag_id, true, newQuery, next_pos,
								(QueryNode) q.parent.parent, id, context, -1,
								-1, true, true);
					} else {
						ActionSequence newChain = new ActionSequence();
						QSQEvaluateQuery.applyRule(newChain, t,
								(QueryNode) q.parent.parent, rule, true,
								strag_id, id, next_pos, newQuery, -1, -1,
								context);
						output.branch(newChain);
					}
				}
			} else {
				Tree t = (Tree) context.getObjectFromCache("tree");
				QueryNode q = t.getQuery(getParamInt(RuleExecutor3.I_QUERY_ID));

				QueryNode newQuery = t.newQuery(q.parent);
				newQuery.s = triple[0].getValue();
				newQuery.p = triple[1].getValue();
				newQuery.o = triple[2].getValue();
				newQuery.sibling = q.sibling;
				q.sibling = newQuery;
				q = newQuery;
				if (!Ruleset.getInstance().getQSQEvaluation()) {
					ReasoningUtils.generate_new_chain(output, rule, strag_id, true,
							newQuery, next_pos, (QueryNode) q.parent.parent, id,
							context, -1, -1, true, true);	
				} else {
					ActionSequence newChain = new ActionSequence();
					QSQEvaluateQuery.applyRule(newChain, t,
							(QueryNode) q.parent.parent, rule, true,
							strag_id, id, next_pos, newQuery, -1, -1,
							context);
					output.branch(newChain);
				}				
			}
		}
	}

	protected CacheCollectionValues getCache(ActionContext context) {
		CacheCollectionValues cache = (CacheCollectionValues) context
				.getObjectFromCache("cache");
		if (cache == null) {
			synchronized (CacheCollectionValues.class) {
				cache = (CacheCollectionValues) context
						.getObjectFromCache("cache");
				if (cache == null) {
					cache = new CacheCollectionValues();
					context.putObjectInCache("cache", cache);
				}
			}
		}
		return cache;
	}
}
