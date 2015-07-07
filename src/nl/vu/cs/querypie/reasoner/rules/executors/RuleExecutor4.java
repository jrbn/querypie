package nl.vu.cs.querypie.reasoner.rules.executors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import nl.vu.cs.querypie.reasoning.expand.RuleNode;
import nl.vu.cs.querypie.reasoning.expand.Tree;
import nl.vu.cs.querypie.reasoning.expand.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.TupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleExecutor4 extends RuleExecutor3 {

	static final Logger log = LoggerFactory.getLogger(RuleExecutor4.class);

	public static final int I_LIST_CURRENT = 10;

	private static final class HeadInfo {
		long head;
		boolean last;
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		super.registerActionParameters(conf);
		conf.registerParameter(I_LIST_CURRENT, "I_LIST_CURRENT", 0, true);
	}

	private Rule4 ruleDef;
	private final Pattern support_p = new Pattern();

	Collection<Long> list_heads;
	int list_current;
	boolean do_join;
	int[] fields_to_match;

	int n_pos_from_precomp;
	Mapping[] pos_from_precomp = new Mapping[3];
	int n_pos_from_triple;
	Mapping[] pos_from_triple = new Mapping[3];

	int nLists;
	int lengthList;
	int nGenPatterns;
	int posElNInPattern;

	Map<Long, Collection<HeadInfo>> listElementsToHeads = new HashMap<>();

	private int[] determineJoinPoints(int pos) throws Exception {
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
			mkey_mapping_actual_tuples_generic.setNumberFields(input.length);
			key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
					.getRawFields();

		} else if (pos != 0) {
			input = new int[ruleDef.lshared_var_with_next_gen_pattern[pos - 1].length + 1];
			fields_to_match = new int[input.length];
			for (int i = 0; i < input.length - 1; ++i) {
				if (ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos1 < 0)
					input[i] = ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos1
					+ actual_precomputed_tuples.getNameBindings()
					.size();
				else
					input[i] = ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos1;
				fields_to_match[i] = ruleDef.lshared_var_with_next_gen_pattern[pos - 1][i].pos2;
			}
			// Find the position of the listhead in the bindings
			int i = 0;
			boolean found = false;
			for (String binding : actual_precomputed_tuples.getNameBindings()) {
				if (binding.equals("listhead")) {
					found = true;
					break;
				}
				i++;
			}

			if (!found) {
				throw new Exception("There must be a listhead to join against.");
			}
			input[input.length - 1] = i;

			// Set the datastructure for the lookup
			mkey_mapping_actual_tuples_generic.setNumberFields(input.length);
			key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
					.getRawFields();

		} else {
			input = new int[ruleDef.lshared_var_new_list.length + 1];
			fields_to_match = new int[input.length];
			for (int i = 0; i < input.length - 1; ++i) {
				if (ruleDef.lshared_var_new_list[i].pos1 < 0)
					input[i] = ruleDef.lshared_var_new_list[i].pos1
					+ actual_precomputed_tuples.getNameBindings()
					.size();
				else
					input[i] = ruleDef.lshared_var_new_list[i].pos1;
				fields_to_match[i] = ruleDef.lshared_var_new_list[i].pos2;
			}
			// Find the position of the listhead in the bindings
			int i = 0;
			boolean found = false;
			for (String binding : actual_precomputed_tuples.getNameBindings()) {
				if (binding.equals("listhead")) {
					found = true;
					break;
				}
				i++;
			}

			if (!found) {
				throw new Exception("There must be a listhead to join against.");
			}
			input[input.length - 1] = i;

			// Set the datastructure for the lookup
			mkey_mapping_actual_tuples_generic
			.setNumberFields(fields_to_match.length);
			key_mapping_actual_tuples_generic = mkey_mapping_actual_tuples_generic
					.getRawFields();
		}
		return input;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		ruleDef = ruleset.getRuleFourthType(getParamInt(I_RULEDEF));
		list_current = getParamInt(I_LIST_CURRENT);
		do_join = false;
		listElementsToHeads.clear();
		posElNInPattern = -1;

		/*** Get info about the number of patterns and size of the list ***/
		Tree tree = (Tree) context
				.getObjectFromCache(getParamString(S_TREENAME));
		QueryNode query = tree.getQuery(getParamInt(I_QUERY_ID));

		ruleNode = (RuleNode) query.parent;
		nGenPatterns = 0;
		list_heads = query.list_heads;
		if (ruleDef.GENERICS_STRATS != null
				&& ruleDef.GENERICS_STRATS[0].patterns != null) {
			nGenPatterns = ruleDef.GENERICS_STRATS[0].patterns.length;
		}
		if (ruleNode.intermediateTuples == null) {
			ruleNode.intermediateTuples = new TupleSet[nGenPatterns];
		}
		nLists = 0;
		lengthList = 0;
		if (list_heads != null) {
			if (ruleDef.LIST_PATTERNS != null) {
				nLists = ruleDef.LIST_PATTERNS.length;
			}
			lengthList = getListsMaxLength(ruleDef.all_lists, list_heads);
			if (ruleNode.listIntermediateTuples == null) {
				ruleNode.listIntermediateTuples = new TupleSet[nLists
				                                               * lengthList];
			}
		}

		// Instantiate the triple in output
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
			// final List<Long> elements = ruleDef.all_lists.get(list_head);
			final int pos = getParamInt(I_PATTERN_POS);
			current_generic_pattern_pos = pos;

			// Set up a mapping between the actual elements of the list that
			// can
			// appear in the input and the corresponding list head. I need
			// this
			// to perform the join between the elements during the process()
			// call.
			createMapElementsToHead(ruleDef.all_lists, query.list_heads,
					list_current, listElementsToHeads);
			Pattern listPattern = ruleDef.LIST_PATTERNS[pos];
			posElNInPattern = listPattern.getPosVar("el_n");

			// Retrieve the current bindings
			if (ruleDef.GENERICS_STRATS != null || pos != 0
					|| list_current != 0) {
				if (list_current == 0 && pos == 0) {
					actual_precomputed_tuples = ruleNode.intermediateTuples[nGenPatterns - 1];
				} else {
					actual_precomputed_tuples = ruleNode.listIntermediateTuples[nLists
					                                                            * list_current + pos - 1];
				}
			} else {
				actual_precomputed_tuples = ruleDef.precomputed_tuples;
			}

			ruleDef.substituteListNameValueInPattern(
					ruleDef.LIST_PATTERNS[pos], support_p.p, list_current, -1);

			newTupleSet = new TupleSet(TupleSet.concatenateBindings(
					actual_precomputed_tuples.getNameBindings(), support_p));

			int[] input = determineJoinPoints(pos);
			if (actual_precomputed_tuples == ruleDef.precomputed_tuples) {
				mapping_generic_actual_tuples = actual_precomputed_tuples
						.getBindingsFromBindings(input, null, true);
			} else {
				mapping_generic_actual_tuples = actual_precomputed_tuples
						.getBindingsFromBindings(input, null);
			}

			// Calculate the positions to retrieve to instantiate the
			// head
			n_pos_from_precomp = 0;
			n_pos_from_triple = 0;
			for (int pos1 : ruleDef.pos_vars_head) {
				RDFTerm t = instantiated_head[pos1];
				RDFTerm original_t = ruleDef.HEAD.p[pos1];
				String nameHeadVar = original_t.getName();
				if (t.getValue() < 0) {
					// Get the position in the precomp_pattern
					boolean found = false;
					List<String> names = newTupleSet.getNameBindings();
					int i = 0;
					for (String name : names) {
						if (name.equals(nameHeadVar)) {
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
									&& p.p[j].getName().equals(nameHeadVar)) {
								Mapping map = new Mapping();
								map.pos1 = j;
								map.pos2 = pos1;
								pos_from_triple[n_pos_from_triple++] = map;
								found = true;
							}
						}
					}

					if (!found) {
						throw new Exception("This case should be checked");
					}
				}
			}
		}

		final int idFilterValues = getParamInt(I_FILTERVALUESSET);
		if (idFilterValues != -1) {
			filterValues = (Set<Long>) context
					.getObjectFromCache("filterValues-" + idFilterValues);
			if (ruleDef.GENERICS_STRATS != null
					&& ruleDef.GENERICS_STRATS.length != 0)
				posToFilter = ruleDef.GENERICS_STRATS[strag_id].posGenFilterBindingsFromHead[0];
			else
				posToFilter = ruleDef.posGenFilterBindingsFromHead;
		} else {
			filterValues = null;
		}
	}

	private void createMapElementsToHead(Map<Long, List<Long>> all_lists,
			Collection<Long> list_heads, int posInList,
			Map<Long, Collection<HeadInfo>> output) {
		for (long head : list_heads) {
			List<Long> list = all_lists.get(head);
			if (list.size() > posInList) {
				long el = list.get(posInList);
				Collection<HeadInfo> listHeads = null;
				if (output.containsKey(el)) {
					listHeads = output.get(el);
				} else {
					listHeads = new ArrayList<>();
					output.put(el, listHeads);
				}
				HeadInfo info = new HeadInfo();
				info.head = head;
				info.last = posInList == list.size() - 1;
				listHeads.add(info);
			}
		}
	}

	private void processList(Tuple inputTuple, ActionOutput output)
			throws Exception {

		if (filterValues != null
				&& filterValues
				.contains(((RDFTerm) inputTuple.get(posToFilter))
						.getValue())) {
			return;
		}

		RDFTerm term;

		// Extract the fields of the tuple to join the precomputed pattern
		for (int i = 0; i < fields_to_match.length - 1; ++i) {
			term = (RDFTerm) inputTuple.get(fields_to_match[i]);
			key_mapping_actual_tuples_generic[i] = term.getValue();
		}

		final CollectionTuples match = mapping_generic_actual_tuples
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

				final int sizeTuple = match.getSizeTuple();
				final long[] rawValues = match.getRawValues();
				for (int y = match.getStart(); y < match.getEnd(); y += sizeTuple) {
					final long[] concat = TupleSet.concatenateTuples(rawValues,
							y, sizeTuple, list_variable_values);
					newTupleSet.addTuple(concat);
				}
			} else {
				final int sizeTuple = match.getSizeTuple();
				final long[] rawValues = match.getRawValues();

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
				// processGenericsIntermediate(inputTuple);
				super.process(inputTuple, context, output);
			}
		} else {
			Collection<HeadInfo> heads = listElementsToHeads
					.get(((RDFTerm) inputTuple.get(posElNInPattern)).getValue());
			for (HeadInfo headInfo : heads) {
				key_mapping_actual_tuples_generic[key_mapping_actual_tuples_generic.length - 1] = headInfo.head;
				do_join = headInfo.last;
				processList(inputTuple, output);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		if (newTupleSet != null && newTupleSet.size() > 0) {
			final Tree t = (Tree) context
					.getObjectFromCache(getParamString(S_TREENAME));
			QueryNode q = t.getQuery(getParamInt(RuleExecutor3.I_QUERY_ID));
			final QueryNode parent = (QueryNode) q.parent.parent;

			if (list_current == -1) {
				if (fetch_next_pattern) {
					if (!generate_chain_for_next_step(ruleDef, context, output)) {
						cleanup(context);
					}
				} else {
					// Copy the bindings
					long prevSize = 0;
					if (ruleNode.intermediateTuples[nGenPatterns - 1] != null)
						prevSize = ruleNode.intermediateTuples[nGenPatterns - 1]
								.size();
					ruleNode.intermediateTuples[nGenPatterns - 1] = newTupleSet;
					// I dont' want to cache rules with generic patterns because
					// the bindings might be different

					// Get all first elements of the lists
					final Collection<Long> heads = newTupleSet.getAllValues(
							"listhead", false);

					// Pos of the list and bindings from the generic pattern in
					// the new pattern
					Mapping[] pos = new Mapping[2];
					pos[0] = new Mapping();
					pos[0].pos2 = ruleDef.LIST_PATTERNS[0].getPosVar("el_n");
					pos[1] = ruleDef.last_generic_first_list_mapping[0];

					// Get all actual bindings
					Collection<Long>[] allBindings = new Collection[2];
					allBindings[0] = getElementsFromLists(ruleDef.all_lists,
							heads, 0);
					allBindings[1] = newTupleSet.getAllValues(pos[1].pos1,
							false);

					// Get previous query
					QueryNode newQuery = q.next != null ? q.next[0] : null;

					// Get shared IDs
					long[] oSharedIds = getSharedIDsForCustomSets(newQuery,
							pos, allBindings, context);

					if (newQuery == null) {
						newQuery = t.newQuery(q.parent);
						newQuery.prev = q;
						newQuery.current_pattern = 0;
						newQuery.list_id = 0;
						newQuery.setS(support_p.p[0].getValue());
						newQuery.p = support_p.p[1].getValue();
						newQuery.o = support_p.p[2].getValue();
						QueryNode[] newQueries = new QueryNode[1];
						newQueries[0] = newQuery;
						q.next = newQueries;
					} else {
						if (newTupleSet.size() > prevSize) {
							t.setToReexpand(newQuery);
							newQuery.forceUpdateInPreviousIteration();
						} else {
							// Continue only if one of the next queries is
							// updated
							QueryNode[] nodes = t
									.getFirstUpdatedAmongNextQueries(newQuery);
							if (nodes == null) {
								cleanup(context);
								return;
							}
							newQuery.forceUpdateInPreviousIteration();
						}
					}
					newQuery.list_heads = heads;

					ruleDef.substituteListNameValueInPattern(
							ruleDef.LIST_PATTERNS[0], newQuery, 0,
							oSharedIds[0]);

					newQuery.setTerm(
							ruleDef.last_generic_first_list_mapping[0].pos2,
							oSharedIds[1]);

					TreeExpander.unify((QueryNode) newQuery.parent.parent,
							newQuery, ruleDef.lshared_var_firsthead[0]);

					if (!Ruleset.getInstance().getQSQEvaluation()) {
						ReasoningUtils.generate_new_chain(output, ruleDef, 0,
								true, newQuery, 0, parent, context, 0, true,
								true, getParamInt(I_RULESET),
								getParamString(S_TREENAME), -1);
					} else {
						final ActionSequence newChain = new ActionSequence();
						QSQEvaluateQuery.applyRule(newChain, t, parent,
								ruleDef, true, strag_id, 0, newQuery, 0, -1,
								context);
						output.branch(newChain);
					}
				}
			} else {
				long prevSize = 0;
				if (ruleNode.listIntermediateTuples[list_current * nLists
				                                    + current_generic_pattern_pos] != null)
					prevSize = ruleNode.listIntermediateTuples[list_current
					                                           * nLists + current_generic_pattern_pos].size();
				ruleNode.listIntermediateTuples[list_current * nLists
				                                + current_generic_pattern_pos] = newTupleSet;

				if (current_generic_pattern_pos == 0
						&& ruleDef.GENERICS_STRATS == null)
					// t.addTupleSetToCache(newTupleSet, parent,
					// (RuleNode) q.parent, q);

					if (current_generic_pattern_pos < ruleDef.LIST_PATTERNS.length - 1) {

						// Pos of the list and bindings from the generic pattern
						// in
						// the new pattern
						Mapping[] pos = new Mapping[2];
						pos[0] = new Mapping();
						pos[0].pos2 = ruleDef.LIST_PATTERNS[0].getPosVar("el_"
								+ list_current);
						pos[1] = ruleDef.last_generic_first_list_mapping[0];

						// Get all actual bindings
						Collection<Long>[] allBindings = new Collection[2];
						allBindings[0] = newTupleSet.getAllValues("el_"
								+ list_current, false);
						allBindings[1] = newTupleSet
								.getAllValues(
										ruleDef.lshared_var_with_next_gen_pattern[current_generic_pattern_pos][0].pos1,
										false);

						// Get previous query
						QueryNode newQuery = q.next != null ? q.next[0] : null;

						// Get shared IDs
						long[] oSharedIds = getSharedIDsForCustomSets(newQuery,
								pos, allBindings, context);

						if (q.next != null) {
							assert (q.next.length == 1);
							newQuery = q.next[0];
							if (newTupleSet.size() > prevSize) {
								t.setToReexpand(newQuery);
								newQuery.forceUpdateInPreviousIteration();
							} else {
								if (t.getFirstUpdatedAmongNextQueries(newQuery) == null) {
									return;
								} else {
									newQuery.forceUpdateInPreviousIteration();
								}
							}
						} else {
							newQuery = t.newQuery(q.parent);
							newQuery.list_heads = list_heads;
							newQuery.list_id = list_current;
							newQuery.current_pattern = current_generic_pattern_pos + 1;
							newQuery.prev = q;
							newQuery.setS(support_p.p[0].getValue());
							newQuery.p = support_p.p[1].getValue();
							newQuery.o = support_p.p[2].getValue();
							QueryNode[] newQueries = new QueryNode[1];
							newQueries[0] = newQuery;
							q.next = newQueries;
						}

						if (list_current == 0) {
							TreeExpander
							.unify(parent,
									newQuery,
									ruleDef.lshared_var_firsthead[current_generic_pattern_pos + 1]);
						} else {
							TreeExpander
							.unify(parent,
									newQuery,
									ruleDef.lshared_var_head[current_generic_pattern_pos + 1]);
						}

						ruleDef.substituteListNameValueInPattern(
								ruleDef.LIST_PATTERNS[current_generic_pattern_pos + 1],
								newQuery, list_current, oSharedIds[0]);
						newQuery.setTerm(
								ruleDef.lshared_var_with_next_gen_pattern[current_generic_pattern_pos][0].pos2,
								oSharedIds[1]);

						if (!Ruleset.getInstance().getQSQEvaluation()) {
							ReasoningUtils.generate_new_chain(output, ruleDef,
									0, true, newQuery,
									current_generic_pattern_pos + 1, parent,
									context, list_current, true, true,
									getParamInt(I_RULESET),
									getParamString(S_TREENAME), -1);
						} else {
							final ActionSequence newChain = new ActionSequence();
							QSQEvaluateQuery.applyRule(newChain, t, parent,
									ruleDef, true, 0,
									current_generic_pattern_pos + 1, newQuery,
									list_current, -1, context);
							output.branch(newChain);
						}
					} else {

						// Move to the next element of the list
						if (ruleDef.lshared_var_new_list.length > 1) {
							throw new Exception("Not supported");
						}

						// Calculate the new set of bindings
						String key = ruleDef.lshared_var_new_list[0].nameBinding;
						if (key.indexOf('_') != -1) {
							key = key.substring(0, key.indexOf('_')) + "_"
									+ (list_current + 1);
						}

						// Get list IDs
						Mapping[] pos = new Mapping[2];
						pos[0] = new Mapping(); // I only need to calculate the
						// position
						// of the
						// list element in the pattern (in pos2). I do
						// it if I need to check the previous value
						// (query.next != null)
						pos[0].pos2 = ruleDef.LIST_PATTERNS[0]
								.getPosVar("el_n");
						pos[1] = ruleDef.lshared_var_new_list[0];

						// Get all actual bindings
						Collection<Long>[] allBindings = new Collection[2];
						allBindings[0] = getElementsFromLists(
								ruleDef.all_lists, list_heads, list_current + 1);
						allBindings[1] = newTupleSet.getAllValues(key, false);

						// Get previous query
						QueryNode newQuery = q.next != null ? q.next[0] : null;

						// Get shared IDs
						long[] oSharedIds = getSharedIDsForCustomSets(newQuery,
								pos, allBindings, context);

						if (q.next != null) {
							assert (q.next.length == 1);
							newQuery = q.next[0];
							if (newTupleSet.size() > prevSize) {
								t.setToReexpand(newQuery);
								newQuery.forceUpdateInPreviousIteration();
							} else {
								if (t.getFirstUpdatedAmongNextQueries(newQuery) == null) {
									return;
								} else {
									newQuery.forceUpdateInPreviousIteration();
								}
							}
						} else {
							newQuery = t.newQuery(q.parent);
							newQuery.prev = q;
							newQuery.list_heads = list_heads;
							newQuery.list_id = list_current + 1;
							newQuery.current_pattern = 0;
							newQuery.setS(support_p.p[0].getValue());
							newQuery.p = support_p.p[1].getValue();
							newQuery.o = support_p.p[2].getValue();
							QueryNode[] newQueries = new QueryNode[1];
							newQueries[0] = newQuery;
							q.next = newQueries;
						}

						if (list_current == 0) {
							TreeExpander
							.unify(parent,
									newQuery,
									ruleDef.lshared_var_firsthead[current_generic_pattern_pos]);
						} else {
							TreeExpander
							.unify(parent,
									newQuery,
									ruleDef.lshared_var_head[current_generic_pattern_pos]);
						}

						ruleDef.substituteListNameValueInPattern(
								ruleDef.LIST_PATTERNS[0], newQuery,
								list_current + 1, oSharedIds[0]);
						newQuery.setTerm(ruleDef.lshared_var_new_list[0].pos2,
								oSharedIds[1]);

						if (!Ruleset.getInstance().getQSQEvaluation()) {
							ReasoningUtils.generate_new_chain(output, ruleDef,
									0, true, newQuery, 0, parent, context,
									list_current + 1, true, true,
									getParamInt(I_RULESET),
									getParamString(S_TREENAME), -1);
						} else {
							final ActionSequence newChain = new ActionSequence();
							QSQEvaluateQuery.applyRule(newChain, t, parent,
									ruleDef, true, 0, 0, newQuery,
									list_current + 1, -1, context);
							output.branch(newChain);
						}
					}
			}
		} else {
			cleanup(context);
		}

		actual_precomputed_tuples = null;
		mapping_generic_actual_tuples = null;
		key_mapping_actual_tuples_generic = null;
		newTupleSet = null;
		filterValues = null;
		ruleNode = null;
		list_heads = null;
		listElementsToHeads.clear();
	}

	public static Collection<Long> getElementsFromLists(
			Map<Long, List<Long>> all_lists, Collection<Long> heads, int pos) {
		TreeSet<Long> els = new TreeSet<>();
		for (long head : heads) {
			List<Long> list = all_lists.get(head);
			if (list.size() > pos) {
				els.add(list.get(pos));
			}
		}
		return els;
	}

	private int getListsMaxLength(Map<Long, List<Long>> all_lists,
			Collection<Long> list_heads) {
		int maxLength = 0;
		for (long head : list_heads) {
			if (all_lists.get(head).size() > maxLength) {
				maxLength = all_lists.get(head).size();
			}
		}
		return maxLength;
	}
}
