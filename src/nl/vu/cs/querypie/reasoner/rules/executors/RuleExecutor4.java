package nl.vu.cs.querypie.reasoner.rules.executors;

import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule4;
import nl.vu.cs.querypie.reasoning.expand.QSQEvaluateQuery;
import nl.vu.cs.querypie.reasoning.expand.QueryNode;
import nl.vu.cs.querypie.reasoning.expand.Tree;
import nl.vu.cs.querypie.reasoning.expand.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.CacheCollectionValues;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.TupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleExecutor4 extends RuleExecutor3 {

	static final Logger log = LoggerFactory.getLogger(RuleExecutor4.class);

	public static final int L_LIST_HEAD = 8;
	public static final int I_LIST_CURRENT = 9;

	@Override
	public void registerActionParameters(ActionConf conf) {
		super.registerActionParameters(conf);
		conf.registerParameter(L_LIST_HEAD, "L_LIST_HEAD", 0, true);
		conf.registerParameter(I_LIST_CURRENT, "I_LIST_CURRENT", 0, true);
	}

	private Rule4 ruleDef;
	private final Pattern support_p = new Pattern();

	long list_head;
	int list_current;
	boolean do_join;
	int[] fields_to_match;

	int n_pos_from_precomp;
	Mapping[] pos_from_precomp = new Mapping[3];
	int n_pos_from_triple;
	Mapping[] pos_from_triple = new Mapping[3];

	@Override
	public void startProcess(ActionContext context) throws Exception {
		ruleDef = ruleset.getRuleFourthType(getParamInt(I_RULEDEF));
		list_head = getParamLong(L_LIST_HEAD);
		list_current = getParamInt(I_LIST_CURRENT);
		do_join = false;

		// Instantiate the output
		for (int i = 0; i < 3; ++i) {
			instantiated_head[i].setValue(getParamLong(i));
			if (ruleDef.HEAD.p[i].getValue() >= 0) {
				triple[i].setValue(ruleDef.HEAD.p[i].getValue());
			} else {
				triple[i].setValue(instantiated_head[i].getValue());
			}
		}

		if (list_current == -1) {
			if (ruleDef.GENERICS_STRATS == null) {
				// Same as rule type 1
				check_head_is_precomputed(ruleDef);
				do_join = true;
			} else {
				strag_id = getParamInt(I_STRAG_ID);
				parse_current_state(ruleDef, ruleDef.GENERICS_STRATS[strag_id],
						ruleDef.LIST_PATTERNS.length > 0, false, context);
				if (ruleDef.LIST_PATTERNS.length == 0
						&& !this.fetch_next_pattern) {
					prepareForJoin(ruleDef, ruleDef.precomputed_tuples,
							ruleDef.GENERICS_STRATS[strag_id],
							instantiated_head, false, context);
					do_join = true;
				}
			}
		} else {
			// I am processing a list block.
			List<Long> elements = ruleDef.all_lists.get(list_head);
			int pos = getParamInt(I_PATTERN_POS);
			current_generic_pattern_pos = pos;
			boolean last_el_in_list = pos == ruleDef.LIST_PATTERNS.length - 1;

			if (last_el_in_list && list_current == elements.size() - 1) {
				do_join = true;
			}

			// Parse the object to store the results in memory
			ruleDef.substituteListNameValueInPattern(
					ruleDef.LIST_PATTERNS[pos], support_p.p, list_current,
					elements.get(list_current));

			// Retrieve the current bindings
			if (ruleDef.GENERICS_STRATS != null || pos != 0
					|| list_current != 0) {
				Object key = getParamInt(I_KEY);
				actual_precomputed_tuples = (TupleSet) context
						.getObjectFromCache(key);
			} else {
				actual_precomputed_tuples = ruleDef.precomputed_tuples;
			}

			if (do_join) {
				// Calculate the positions to retrieve to instantiate the
				// head
				n_pos_from_precomp = 0;
				n_pos_from_triple = 0;
				for (int pos1 : ruleDef.pos_vars_head) {
					RDFTerm t = instantiated_head[pos1];
					RDFTerm original_t = ruleDef.HEAD.p[pos1];
					if (t.getValue() < 0) {
						// Get the position in the precomp_pattern
						boolean found = false;
						List<String> names = actual_precomputed_tuples
								.getNameBindings();
						int i = 0;
						for (String name : names) {
							if (name.equals(original_t.getName())) {
								Mapping map = new Mapping();
								map.pos1 = i;
								map.pos2 = pos1;
								pos_from_precomp[n_pos_from_precomp++] = map;
								found = true;
								break;
							}
							++i;
						}

						if (!found) {
							Pattern p = ruleDef.LIST_PATTERNS[ruleDef.LIST_PATTERNS.length - 1];
							for (int j = 0; j < 3; ++j) {
								if (p.p[j].getName() != null
										&& p.p[j].getName().equals(
												original_t.getName())) {
									Mapping map = new Mapping();
									map.pos1 = j;
									map.pos2 = pos1;
									pos_from_triple[n_pos_from_triple++] = map;
									found = true;
								}
							}
						}

						if (!found) {
							throw new Exception("This should not happen");
						}
					}
				}
			}

			// Determine the join points
			int[] input = null;
			if (pos == 0 && list_current == 0) {

				input = new int[ruleDef.last_generic_first_list_mapping.length];
				fields_to_match = new int[input.length - 1];
				int m = 0;

				for (int i = 0; i < input.length; ++i) {
					if (!ruleDef.last_generic_first_list_mapping[i].nameBinding
							.equals("el_n")) {
						input[m] = ruleDef.last_generic_first_list_mapping[i].pos1;
						fields_to_match[m] = ruleDef.last_generic_first_list_mapping[i].pos2;
					} else {
						input[input.length - 1] = ruleDef.last_generic_first_list_mapping[i].pos1;
					}
					m++;
				}

				// Set the datastructure for the lookup
				mkey_mapping_actual_tuples_generic
						.setNumberFields(input.length);
				key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
						.getRawFields();
				key_mapping_actual_tuples_generic[input.length - 1] = list_head;

			} else if (pos != 0) {
				input = new int[ruleDef.lshared_var_with_next_gen_pattern[pos - 1].length];
				fields_to_match = new int[input.length];
				for (int i = 0; i < input.length; ++i) {
					if (ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos1 < 0)
						input[i] = ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos1
								+ actual_precomputed_tuples.getNameBindings()
										.size();
					else
						input[i] = ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos1;
					fields_to_match[i] = ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos2;
				}

				// Set the datastructure for the lookup
				mkey_mapping_actual_tuples_generic
						.setNumberFields(input.length);
				key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
						.getRawFields();

			} else {
				input = new int[ruleDef.lshared_var_new_list.length];
				fields_to_match = new int[input.length];
				for (int i = 0; i < input.length; ++i) {
					if (ruleDef.lshared_var_new_list[i].pos1 < 0)
						input[i] = ruleDef.lshared_var_new_list[i].pos1
								+ actual_precomputed_tuples.getNameBindings()
										.size();
					else
						input[i] = ruleDef.lshared_var_new_list[i].pos1;
					fields_to_match[i] = ruleDef.lshared_var_new_list[i].pos2;
				}

				// Set the datastructure for the lookup
				mkey_mapping_actual_tuples_generic
						.setNumberFields(input.length);
				key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
						.getRawFields();
			}

			if (actual_precomputed_tuples == ruleDef.precomputed_tuples) {
				mapping_generic_actual_tuples = actual_precomputed_tuples
						.getBindingsFromBindings(input, null, true);
			} else {
				mapping_generic_actual_tuples = actual_precomputed_tuples
						.getBindingsFromBindings(input, null);
			}

			// Create new dataset
			if (!do_join) {
				newTupleSet = new TupleSet(TupleSet.concatenateBindings(
						actual_precomputed_tuples.getNameBindings(), support_p));
			}
		}
	}

	private void processList(Tuple inputTuple, ActionOutput output)
			throws Exception {

		RDFTerm term;

		// Extract the fields of the tuple to join the precomputed pattern
		for (int i = 0; i < fields_to_match.length; ++i) {
			term = (RDFTerm) inputTuple.get(fields_to_match[i]);
			key_mapping_actual_tuples_generic[i] = term.getValue();
		}

		CollectionTuples match = mapping_generic_actual_tuples
				.get(mkey_mapping_actual_tuples_generic);
		if (match != null) {
			if (!do_join) {
				long[] list_variable_values = null;

				if (list_current == 0) {
					list_variable_values = new long[ruleDef.pos_unique_vars[current_generic_pattern_pos].length];
					for (int i = 0; i < ruleDef.pos_unique_vars[current_generic_pattern_pos].length; ++i) {

						term = (RDFTerm) inputTuple
								.get(ruleDef.pos_unique_vars[current_generic_pattern_pos][i]);
						list_variable_values[i] = term.getValue();
					}
				} else {
					list_variable_values = new long[ruleDef.pos_unique_vars_next[current_generic_pattern_pos].length];
					for (int i = 0; i < ruleDef.pos_unique_vars_next[current_generic_pattern_pos].length; ++i) {
						term = (RDFTerm) inputTuple
								.get(ruleDef.pos_unique_vars_next[current_generic_pattern_pos][i]);
						list_variable_values[i] = term.getValue();
					}
				}

				int sizeTuple = match.getSizeTuple();
				long[] rawValues = match.getRawValues();
				for (int y = match.getStart(); y < match.getEnd(); y += sizeTuple) {
					long[] concat = TupleSet.concatenateTuples(rawValues, y,
							sizeTuple, list_variable_values);
					newTupleSet.addTuple(concat);
				}
			} else {
				int sizeTuple = match.getSizeTuple();
				long[] rawValues = match.getRawValues();

				for (int i = 0; i < n_pos_from_triple; ++i) {
					term = (RDFTerm) inputTuple.get(pos_from_triple[i].pos1);
					triple[pos_from_triple[i].pos2].setValue(term.getValue());
				}

				for (int y = match.getStart(); y < match.getEnd(); y += sizeTuple) {
					for (int i = 0; i < n_pos_from_precomp; ++i) {
						triple[pos_from_precomp[i].pos2].setValue(rawValues[y
								+ pos_from_precomp[i].pos1]);
					}
					outputTuple(triple, output);
				}
			}
		}
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (list_current == -1) {
			if (ruleDef.GENERICS_STRATS == null) {
				process_precomputed(ruleDef, inputTuple, output, context);
			} else {
				super.process(inputTuple, context, output);
			}
		} else {
			processList(inputTuple, output);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {

		if (!do_join) {

			if (newTupleSet.size() == 0)
				return;

			if (list_current == -1) {
				if (fetch_next_pattern) {
					generate_chain_for_next_step(ruleDef, context, output);
				} else {
					// Extract the variables from the generic patterns
					Collection<Long> values = newTupleSet
							.getAllValues(
									ruleDef.last_generic_first_list_mapping[0].nameBinding,
									false);
					// Put them in the submission cache
					int id = context.getNewBucketID();
					context.putObjectInCache(id, newTupleSet);

					long shared_set_id;
					CacheCollectionValues cache = getCache(context);
					CacheCollectionValues.Entry entry = cache
							.getCollection(values);
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

					Collection<Long> heads = newTupleSet.getAllValues(
							"listhead", false);

					Tree t = (Tree) context.getObjectFromCache("tree");
					QueryNode q = t
							.getQuery(getParamInt(RuleExecutor3.I_QUERY_ID));

					for (long list_head : heads) {
						// Read the first element of the list
						List<Long> elements = ruleDef.all_lists.get(list_head);

						QueryNode newQuery = t.newQuery(q.parent);
						newQuery.s = support_p.p[0].getValue();
						newQuery.p = support_p.p[1].getValue();
						newQuery.o = support_p.p[2].getValue();
						newQuery.sibling = q.sibling;
						q.sibling = newQuery;
						q = newQuery;

						ruleDef.substituteListNameValueInPattern(
								ruleDef.LIST_PATTERNS[0], newQuery, 0,
								elements.get(0));

						TreeExpander.unify((QueryNode) newQuery.parent.parent,
								newQuery, ruleDef.lshared_var_firsthead[0]);

						newQuery.setTerm(
								ruleDef.last_generic_first_list_mapping[0].pos2,
								shared_set_id);
						if (!Ruleset.getInstance().getQSQEvaluation()) {
							ReasoningUtils.generate_new_chain(output, ruleDef,
									0, true, newQuery, 0, q, id,
									context, list_head, 0, true, true);
						} else {
							ActionSequence newChain = new ActionSequence();
							QSQEvaluateQuery.applyRule(newChain, t, q, ruleDef,
									true, strag_id, id, 0,
									newQuery, list_head, 0, context);
							output.branch(newChain);
						}
					}

				}
			} else {
				List<Long> elements = ruleDef.all_lists.get(list_head);
				if (current_generic_pattern_pos < ruleDef.LIST_PATTERNS.length - 1) {

					// Simply increase the counter
					if (ruleDef.lshared_var_with_next_gen_pattern[current_generic_pattern_pos].length > 1) {
						log.debug("The join will be performed only on one term. The other will be performed later.");
					}

					// Broadcast the tuple set
					Collection<Long> values = newTupleSet
							.getAllValues(
									ruleDef.lshared_var_with_next_gen_pattern[current_generic_pattern_pos][0].nameBinding,
									false);
					// Put them in the submission cache
					int id = context.getNewBucketID();
					context.putObjectInCache(id, newTupleSet);

					CacheCollectionValues cache = getCache(context);
					CacheCollectionValues.Entry entry = cache
							.getCollection(values);
					long shared_set_id;
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

					Tree t = (Tree) context.getObjectFromCache("tree");
					QueryNode q = t
							.getQuery(getParamInt(RuleExecutor3.I_QUERY_ID));
					QueryNode parent = (QueryNode) q.parent.parent;

					QueryNode newQuery = t.newQuery(q.parent);
					newQuery.s = support_p.p[0].getValue();
					newQuery.p = support_p.p[1].getValue();
					newQuery.o = support_p.p[2].getValue();
					newQuery.sibling = q.sibling;
					q.sibling = newQuery;
					q = newQuery;

					ruleDef.substituteListNameValueInPattern(
							ruleDef.LIST_PATTERNS[current_generic_pattern_pos + 1],
							newQuery, list_current, elements.get(list_current));

					if (list_current == 0)
						TreeExpander
								.unify(parent,
										newQuery,
										ruleDef.lshared_var_firsthead[current_generic_pattern_pos + 1]);
					else if (list_current == elements.size() - 1) {
						TreeExpander
								.unify(parent,
										newQuery,
										ruleDef.lshared_var_lasthead[current_generic_pattern_pos + 1]);
					} else {
						TreeExpander
								.unify(parent,
										newQuery,
										ruleDef.lshared_var_head[current_generic_pattern_pos + 1]);
					}

					support_p.p[ruleDef.lshared_var_with_next_gen_pattern[current_generic_pattern_pos][0].pos2]
							.setValue(shared_set_id);

					if (!Ruleset.getInstance().getQSQEvaluation()) {
						ReasoningUtils.generate_new_chain(output, ruleDef, 0,
								true, newQuery,
								current_generic_pattern_pos + 1, parent, id,
								context, list_head, list_current, true, true);
					} else {
						ActionSequence newChain = new ActionSequence();
						QSQEvaluateQuery.applyRule(newChain, t, parent, ruleDef,
								true, 0, id, current_generic_pattern_pos + 1, newQuery,
								list_head, list_current, context);
						output.branch(newChain);
					}
				} else {

					// Move to the next element of the list
					if (ruleDef.lshared_var_new_list.length > 1) {
						throw new Exception("Not supported");
					}

					// Broadcast the tuple set
					String key = ruleDef.lshared_var_new_list[0].nameBinding;
					if (key.indexOf('_') != -1) {
						key = key.substring(0, key.indexOf('_')) + "_"
								+ (list_current + 1);
					}

					Collection<Long> values = newTupleSet.getAllValues(key,
							false);
					// Put them in the submission cache
					int id = context.getNewBucketID();
					context.putObjectInCache(id, newTupleSet);
					long shared_set_id;

					CacheCollectionValues cache = getCache(context);
					CacheCollectionValues.Entry entry = cache
							.getCollection(values);
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

					Tree t = (Tree) context.getObjectFromCache("tree");
					QueryNode q = t
							.getQuery(getParamInt(RuleExecutor3.I_QUERY_ID));
					QueryNode parent = (QueryNode) q.parent.parent;

					QueryNode newQuery = t.newQuery(q.parent);
					newQuery.s = support_p.p[0].getValue();
					newQuery.p = support_p.p[1].getValue();
					newQuery.o = support_p.p[2].getValue();
					newQuery.sibling = q.sibling;
					q.sibling = newQuery;
					q = newQuery;

					ruleDef.substituteListNameValueInPattern(
							ruleDef.LIST_PATTERNS[0], newQuery,
							list_current + 1, elements.get(list_current + 1));

					if (list_current == elements.size() - 1) {
						TreeExpander.unify(parent, newQuery,
								ruleDef.lshared_var_lasthead[0]);
					} else {
						TreeExpander.unify(parent, newQuery,
								ruleDef.lshared_var_head[0]);
					}

					support_p.p[ruleDef.lshared_var_new_list[0].pos2]
							.setValue(shared_set_id);

					tuple.set(support_p.p);

					if (!Ruleset.getInstance().getQSQEvaluation()) {
						ReasoningUtils.generate_new_chain(output, ruleDef, 0, true,
								newQuery, 0, parent, id, context, list_head,
								list_current + 1, true, true);
					} else {
						ActionSequence newChain = new ActionSequence();
						QSQEvaluateQuery.applyRule(newChain, t, parent, ruleDef,
								true, 0, id, 0, newQuery,
								list_head, list_current + 1, context);
						output.branch(newChain);
					}

				}
			}
		}
		super.stopProcess(context, output);
	}
}
