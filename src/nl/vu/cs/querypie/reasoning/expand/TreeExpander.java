package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.Rule2.GenericVars;
import nl.vu.cs.querypie.reasoner.rules.Rule3;
import nl.vu.cs.querypie.reasoner.rules.Rule4;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.MultiValue;

public class TreeExpander {

	private static final Schema schema = Schema.getInstance();
	private static final Ruleset ruleset = Ruleset.getInstance();

	public static final int ONLY_FIRST_SECOND = 0;
	public static final int ONLY_THIRD_FOURTH = 1;
	public static final int ALL = 2;

	public static final void expandQuery(ActionContext context,
			QueryNode query, Tree tree, int typeRules) throws Exception {
		// Check whether the subject has a special flag
		if (query.s == Schema.SCHEMA_SUBSET) {
			return;
		}

		if (query.equalsInAncestors(context)) {
			return;
		}

		RuleNode existingRules = (RuleNode) query.child;
		RuleNode lastRule = null;
		MultiValue k1 = new MultiValue(new long[1]);
		MultiValue k2 = new MultiValue(new long[2]);

		if (typeRules != ONLY_THIRD_FOURTH) {
			for (Rule1 rule : ruleset.getAllActiveFirstTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}
				RuleNode c = applyRuleFirstType(rule, query, tree);

				if (lastRule == null) {
					query.child = c;
				} else {
					lastRule.sibling = c;
				}
				lastRule = c;
			}

			for (Rule2 rule : ruleset.getAllActiveSecondTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}
				RuleNode c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
						query, context, k1, k2, tree);

				if (c != null) {
					if (lastRule == null) {
						query.child = c;
					} else {
						lastRule.sibling = c;
					}
					lastRule = c;
				}
			}
		}

		if (typeRules != ONLY_FIRST_SECOND) {
			for (Rule3 rule : ruleset.getAllActiveThirdTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}
				RuleNode c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
						query, context, k1, k2, tree);

				if (c != null) {
					if (lastRule == null) {
						query.child = c;
					} else {
						lastRule.sibling = c;
					}
					lastRule = c;
				}
			}

			for (Rule4 rule : ruleset.getAllActiveFourthTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}

				RuleNode c = null;
				if (rule.GENERICS_STRATS == null && rule.LIST_PATTERNS == null) {
					c = applyRuleFirstType(rule, query, tree);
				} else if (rule.GENERICS_STRATS != null) {
					c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
							query, context, k1, k2, tree);
				} else {
					c = applyRuleWithGenericsList(rule, query, context, k1,
							tree);
				}

				if (c != null) {
					if (lastRule == null) {
						query.child = c;
					} else {
						lastRule.sibling = c;
					}
					lastRule = c;
				}
			}
		}

		if (lastRule != null) {
			lastRule.sibling = existingRules;
		}
	}

	private static final RuleNode applyRuleFirstType(Rule1 rule,
			QueryNode head, Tree tree) throws Exception {
		RuleNode child = tree.newRule(head, rule);

		// Set the query
		QueryNode childQuery = tree.newQuery(child);
		childQuery.s = Schema.SCHEMA_SUBSET;
		child.child = childQuery;

		return child;
	}

	private static final boolean checkHead(Rule1 rule, QueryNode query,
			ActionContext context) {
		boolean ok = true;
		for (int i = 0; i < 3; ++i) {
			long t = 0;
			switch (i) {
			case 0:
				t = query.s;
				break;
			case 1:
				t = query.p;
				break;
			case 2:
				t = query.o;
				break;
			}

			RDFTerm head_value = rule.HEAD.p[i];
			if (RDFTerm.isSet(t)) {
				if (head_value.getValue() >= 0) {
					if (!schema.chekValueInSet(t, head_value.getValue(),
							context)) {
						ok = false;
						break;
					}
				} else if (rule.head_precomputed_id_sets[i] <= Schema.SET_THRESHOLD) {
					if (!schema.isIntersection(t,
							rule.head_precomputed_id_sets[i], context)) {
						ok = false;
						break;
					}
				}
			} else if (t >= 0) {
				if (head_value.getValue() != t
						&& head_value.getValue() != Schema.ALL_RESOURCES) {
					ok = false;
					break;
				}

				if (rule.head_excludedValues != null
						&& rule.head_excludedValues[i].contains(t)) {
					ok = false;
					break;
				}
			}
		}
		return ok;
	}

	private static final int calculate_best_strategy(GenericVars[] generics,
			QueryNode head) {

		if (generics.length == 1) { // No options
			return 0;
		}

		int bestStrategy = 0;
		int nVars = Integer.MAX_VALUE;

		for (int currentStrategy = 0; currentStrategy < generics.length; ++currentStrategy) {
			GenericVars gv = generics[currentStrategy];

			int n_current_vars = gv.pos_variables_generic_patterns[currentStrategy].length;

			Mapping[] pos_head = gv.pos_shared_vars_generics_head[0];
			if (pos_head != null)
				for (Mapping map : pos_head) {
					long v = 0;
					switch (map.pos2) {
					case 0:
						v = head.s;
						break;
					case 1:
						v = head.p;
						break;
					case 2:
						v = head.o;
						break;
					}

					if (v != Schema.ALL_RESOURCES) {
						n_current_vars--;
					}
				}

			if (n_current_vars < nVars) {
				bestStrategy = currentStrategy;
				nVars = n_current_vars;
			}

		}

		return bestStrategy;
	}

	private static final void unify(Pattern pattern, QueryNode triple) {
		// Unify tripleOutput with the values of the GENERIC PATTERN
		triple.s = pattern.p[0].getValue();
		triple.p = pattern.p[1].getValue();
		triple.o = pattern.p[2].getValue();
	}

	public static final void unify(QueryNode input, QueryNode output,
			Mapping[] positions) {
		if (positions != null) {
			for (Mapping pos : positions) {
				output.setTerm(pos.pos1, input.getTerm(pos.pos2));
			}
		}
	}

	private static final RuleNode applyRuleWithGenericsList(Rule4 rule,
			QueryNode head, ActionContext context, MultiValue k1, Tree tree)
			throws Exception {

		if (rule.precomputed_patterns_head != null
				&& rule.precomputed_patterns_head.length == 2) {
			throw new Exception("Not supported");
		}

		RuleNode output = tree.newRule(head, rule);

		// Unify the pattern with the variables that come from the precomputed
		// patterns
		boolean firstOption = rule.precomputed_patterns_head.length == 0
				|| (rule.precomputed_patterns_head.length == 1 && head
						.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES);

		// Get the possible lists that is possible to follow.
		Collection<Long> list_heads = null;
		if (firstOption) {
			list_heads = rule.all_lists.keySet();

			QueryNode lastQuery = null;
			for (long list_head : list_heads) {
				List<Long> list = rule.all_lists.get(list_head);
				if (list != null) {

					QueryNode query = tree.newQuery(output);
					query.list_head = list_head;
					query.list_id = 0;

					rule.substituteListNameValueInPattern(
							rule.LIST_PATTERNS[0], query, 0, list.get(0));
					unify(head, query, rule.lshared_var_firsthead[0]);

					if (lastQuery == null) {
						output.child = query;
					} else {
						lastQuery.sibling = query;
					}
					lastQuery = query;
				}
			}

		} else {
			k1.values[0] = head.getTerm(rule.precomputed_patterns_head[0].pos2);
			CollectionTuples col = rule.mapping_head_list_heads.get(k1);

			if (col != null) {

				QueryNode lastQuery = null;
				for (int i = col.getStart(); i < col.getEnd(); ++i) {
					long list_head = col.getRawValues()[i];
					List<Long> list = rule.all_lists.get(list_head);
					if (list != null) {
						QueryNode query = tree.newQuery(output);
						query.list_head = list_head;
						query.list_id = 0;

						rule.substituteListNameValueInPattern(
								rule.LIST_PATTERNS[0], query, 0, list.get(0));
						unify(head, query, rule.lshared_var_firsthead[0]);

						if (lastQuery == null) {
							output.child = query;
						} else {
							lastQuery.sibling = query;
						}
						lastQuery = query;
					}
				}
			}
		}

		return (output.child == null) ? null : output;
	}

	private static final RuleNode applyRuleWithGenerics(Rule1 rule,
			GenericVars[] generics, QueryNode head, ActionContext context,
			MultiValue k1, MultiValue k2, Tree tree) throws Exception {
		// First determine what is the best strategy to execute the patterns
		// given a instantiated HEAD
		int strategy = calculate_best_strategy(generics, head);
		GenericVars g = generics[strategy];
		return applyRuleWithGenerics(rule, strategy, g, g.patterns[0], -1, -1,
				head, context, k1, k2, tree);

	}

	private static final RuleNode applyRuleWithGenerics(Rule1 rule,
			int strategy, GenericVars g, Pattern p, long list_head,
			int list_id, QueryNode instantiated_head, ActionContext context,
			MultiValue k1, MultiValue k2, Tree tree) throws Exception {

		RuleNode output = tree.newRule(instantiated_head, rule);

		output.strag_id = strategy;
		output.single_node = rule.type != 2;

		// Unify the pattern with the variables that come from the precomputed
		// patterns
		boolean firstOption = rule.precomputed_tuples == null
				|| rule.precomputed_patterns_head.length == 0
				|| (rule.precomputed_patterns_head.length == 1 && instantiated_head
						.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES)
				|| (rule.precomputed_patterns_head.length == 2
						&& instantiated_head
								.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES && instantiated_head
						.getTerm(rule.precomputed_patterns_head[1].pos2) == Schema.ALL_RESOURCES);

		if (firstOption) {
			QueryNode tripleOutput = tree.newQuery(output);
			tripleOutput.list_head = list_head;
			tripleOutput.list_id = list_id;

			// Unify tripleOutput with the values of the GENERIC PATTERN
			unify(p, tripleOutput);

			// Unify the triple with the values from the head that are not used
			// anywhere else
			unify(instantiated_head, tripleOutput,
					g.pos_shared_vars_generics_head[0]);

			if (g.pos_shared_vars_precomp_generics[0].length > 0) {
				tripleOutput.setTerm(
						g.pos_shared_vars_precomp_generics[0][0].pos2,
						g.id_all_values_first_generic_pattern);
			}

			if (g.pos_shared_vars_precomp_generics[0].length > 1) {
				tripleOutput.setTerm(
						g.pos_shared_vars_precomp_generics[0][1].pos2,
						g.id_all_values_first_generic_pattern2);
			}

			output.child = tripleOutput;
			return output;
		} else {

			if (rule.precomputed_patterns_head.length == 1) {
				long v = instantiated_head
						.getTerm(rule.precomputed_patterns_head[0].pos2);
				Collection<Long> possibleKeys = null;
				if (v >= 0) {
					possibleKeys = new ArrayList<Long>();
					possibleKeys.add(v);
				} else {
					possibleKeys = schema.getSubset(v, context);
				}

				QueryNode lastQuery = null;
				for (long key : possibleKeys) {
					k1.values[0] = key;
					CollectionTuples possibleBindings = g.mapping_head_first_generic
							.get(k1);
					if (possibleBindings != null) {
						for (int y = 0; y < possibleBindings.getNTuples(); ++y) {

							if (g.filter_values_last_generic_pattern
									&& v == possibleBindings.getValue(y, 0)) {
								continue;
							}

							QueryNode newQuery = tree.newQuery(output);
							newQuery.list_head = list_head;
							newQuery.list_id = list_id;
							if (output.child == null) {
								unify(p, newQuery);
								unify(instantiated_head, newQuery,
										g.pos_shared_vars_generics_head[0]);
								output.child = newQuery;
							} else {
								// Copy old values
								newQuery.s = lastQuery.s;
								newQuery.p = lastQuery.p;
								newQuery.o = lastQuery.o;
								lastQuery.sibling = newQuery;
							}
							lastQuery = newQuery;

							for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
								lastQuery
										.setTerm(
												g.pos_shared_vars_precomp_generics[0][i].pos2,
												possibleBindings.getValue(y, i));
							}
						}
					}
				}
				return output.child == null ? null : output;
			} else if (rule.precomputed_patterns_head.length == 2) {

				if (instantiated_head
						.getTerm(rule.precomputed_patterns_head[0].pos2) >= 0
						&& instantiated_head
								.getTerm(rule.precomputed_patterns_head[1].pos2) >= 0) {
					// Use both values to restrict the search
					k2.values[0] = instantiated_head
							.getTerm(rule.precomputed_patterns_head[0].pos2);
					k2.values[1] = instantiated_head
							.getTerm(rule.precomputed_patterns_head[1].pos2);
					CollectionTuples possibleBindings = g.mapping_head_first_generic
							.get(k2);
					if (possibleBindings != null) {
						QueryNode lastQuery = null;
						for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
							if (output.child == null) {
								// Unify tripleOutput with the values of the
								// GENERIC PATTERN
								lastQuery = tree.newQuery(output);
								lastQuery.list_head = list_head;
								lastQuery.list_id = list_id;
								unify(p, lastQuery);

								// Unify the triple with the values from the
								// head that are not used
								// anywhere else
								unify(instantiated_head, lastQuery,
										g.pos_shared_vars_generics_head[0]);

								output.child = lastQuery;
							} else {
								QueryNode newQuery = tree.newQuery(output);
								newQuery.list_head = list_head;
								newQuery.list_id = list_id;
								lastQuery.sibling = newQuery;
								newQuery.s = lastQuery.s;
								newQuery.p = lastQuery.p;
								newQuery.o = lastQuery.o;
								lastQuery = newQuery;
							}

							for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
								lastQuery
										.setTerm(
												g.pos_shared_vars_precomp_generics[0][i].pos2,
												possibleBindings.getValue(y, i));
							}

						}
						return output.child == null ? null : output;
					}
				} else {
					CollectionTuples possibleBindings = null;

					if (instantiated_head
							.getTerm(rule.precomputed_patterns_head[0].pos2) >= 0
							&& instantiated_head
									.getTerm(rule.precomputed_patterns_head[1].pos2) == Schema.ALL_RESOURCES) {
						k1.values[0] = instantiated_head
								.getTerm(rule.precomputed_patterns_head[0].pos2);
						possibleBindings = g.mapping_head1_first_generic
								.get(k1);
					} else if (instantiated_head
							.getTerm(rule.precomputed_patterns_head[1].pos2) >= 0
							&& instantiated_head
									.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES) {
						k1.values[0] = instantiated_head
								.getTerm(rule.precomputed_patterns_head[1].pos2);
						possibleBindings = g.mapping_head2_first_generic
								.get(k1);
					} else {
						if (instantiated_head
								.getTerm(rule.precomputed_patterns_head[0].pos2) <= Schema.SET_THRESHOLD
								&& instantiated_head
										.getTerm(rule.precomputed_patterns_head[1].pos2) == Schema.ALL_RESOURCES) {

							long idSet = instantiated_head
									.getTerm(rule.precomputed_patterns_head[0].pos2);
							Collection<Long> col = schema.getSubset(idSet,
									context);
							if (col != null) {

								TreeSet<Long> o = new TreeSet<Long>();

								if (g.pos_shared_vars_precomp_generics[0].length > 1) {
									throw new Exception("Not implemented");
								}

								for (long possibleValue : col) {
									k1.values[0] = possibleValue;
									possibleBindings = g.mapping_head1_first_generic
											.get(k1);
									if (possibleBindings != null) {
										for (int y = 0; y < possibleBindings
												.getNTuples(); ++y) {
											o.add(possibleBindings.getValue(y,
													0));
										}
									}
								}

								if (o.size() > 0) {
									long shared_set_id = ((long) (context
											.getNewBucketID() * -1)) << 16;
									context.putObjectInCache(shared_set_id, o);
									context.broadcastCacheObjects(shared_set_id);

									// Unify tripleOutput with the values of the
									// GENERIC PATTERN
									QueryNode supportTriple = tree
											.newQuery(output);
									supportTriple.list_head = list_head;
									supportTriple.list_id = list_id;
									unify(p, supportTriple);

									// Unify the triple with the values from the
									// head that are not used
									// anywhere else
									unify(instantiated_head, supportTriple,
											g.pos_shared_vars_generics_head[0]);

									supportTriple
											.setTerm(
													g.pos_shared_vars_precomp_generics[0][0].pos2,
													shared_set_id);

									output.child = supportTriple;
									return output;
								}
							}
						} else if (instantiated_head
								.getTerm(rule.precomputed_patterns_head[1].pos2) <= Schema.SET_THRESHOLD
								&& instantiated_head
										.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES) {
							long idSet = instantiated_head
									.getTerm(rule.precomputed_patterns_head[1].pos2);

							Collection<Long> col = schema.getSubset(idSet,
									context);
							if (col != null) {

								TreeSet<Long> o = new TreeSet<Long>();

								if (g.pos_shared_vars_precomp_generics[0].length > 1) {
									throw new Exception("Not implemented");
								}

								for (long possibleValue : col) {
									k1.values[0] = possibleValue;
									possibleBindings = g.mapping_head2_first_generic
											.get(k1);
									if (possibleBindings != null) {
										for (int y = 0; y < possibleBindings
												.getNTuples(); ++y) {
											o.add(possibleBindings.getValue(y,
													0));
										}
									}
								}

								if (o.size() > 0) {
									long shared_set_id = ((long) (context
											.getNewBucketID() * -1)) << 16;
									context.putObjectInCache(shared_set_id, o);
									context.broadcastCacheObjects(shared_set_id);

									// Unify tripleOutput with the values of the
									// GENERIC PATTERN
									QueryNode supportTriple = tree
											.newQuery(output);
									supportTriple.list_head = list_head;
									supportTriple.list_id = list_id;
									unify(p, supportTriple);

									// Unify the triple with the values from the
									// head that are not used
									// anywhere else
									unify(instantiated_head, supportTriple,
											g.pos_shared_vars_generics_head[0]);

									supportTriple
											.setTerm(
													g.pos_shared_vars_precomp_generics[0][0].pos2,
													shared_set_id);
									output.child = supportTriple;
									return output;
								}
							}
						} else {
							Collection<Long> col1 = Schema
									.getInstance()
									.getSubset(
											instantiated_head
													.getTerm(rule.precomputed_patterns_head[0].pos2),
											context);
							Collection<Long> col2 = Schema
									.getInstance()
									.getSubset(
											instantiated_head
													.getTerm(rule.precomputed_patterns_head[1].pos2),
											context);
							if (col1 != null && col2 != null) {
								// Calculate all the set of keys.
								QueryNode lastQuery = null;
								for (long value1 : col1) {
									for (long value2 : col2) {
										k2.values[0] = value1;
										k2.values[1] = value2;
										CollectionTuples col = g.mapping_head3_first_generic
												.get(k2);
										if (col != null) {
											int sizeTuple = col.getSizeTuple();
											for (int i = col.getStart(); i < col
													.getEnd(); i += sizeTuple) {

												if (output.child == null) {
													lastQuery = tree
															.newQuery(output);
													lastQuery.list_head = list_head;
													lastQuery.list_id = list_id;
													unify(p, lastQuery);
													unify(instantiated_head,
															lastQuery,
															g.pos_shared_vars_generics_head[0]);
													output.child = lastQuery;
												} else {
													QueryNode newQuery = tree
															.newQuery(output);
													newQuery.list_head = list_head;
													newQuery.list_id = list_id;
													lastQuery.sibling = newQuery;
													newQuery.s = lastQuery.s;
													newQuery.p = lastQuery.p;
													newQuery.o = lastQuery.o;
													lastQuery = newQuery;
												}

												for (int j = 0; j < sizeTuple; ++j) {

													lastQuery
															.setTerm(
																	g.pos_shared_vars_precomp_generics[0][j].pos2,
																	col.getRawValues()[i]);
												}
											}
										}
									}
								}
								return output;
							}
						}
						return null;
					}

					if (possibleBindings != null) {
						QueryNode lastQuery = null;
						for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
							if (output.child == null) {
								lastQuery = tree.newQuery(output);
								lastQuery.list_head = list_head;
								lastQuery.list_id = list_id;
								unify(p, lastQuery);
								unify(instantiated_head, lastQuery,
										g.pos_shared_vars_generics_head[0]);
								output.child = lastQuery;
							} else {
								QueryNode newQuery = tree.newQuery(output);
								newQuery.list_head = list_head;
								newQuery.list_id = list_id;
								lastQuery.sibling = newQuery;
								newQuery.s = lastQuery.s;
								newQuery.p = lastQuery.p;
								newQuery.o = lastQuery.o;
								lastQuery = newQuery;
							}

							for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
								lastQuery
										.setTerm(
												g.pos_shared_vars_precomp_generics[0][i].pos2,
												possibleBindings.getValue(y, i));
							}
						}
						return output.child == null ? null : output;
					}
				}
			} else {
				throw new Exception("Not supported");
			}
		}

		return null;
	}
}
