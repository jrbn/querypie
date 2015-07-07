package nl.vu.cs.querypie.reasoning.expand;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.Rule2.GenericVars;
import nl.vu.cs.querypie.reasoner.rules.Rule3;
import nl.vu.cs.querypie.reasoner.rules.Rule4;
import nl.vu.cs.querypie.reasoner.rules.executors.RuleExecutor4;
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
	public static final int ONLY_FIRST_SECOND_RECORD_OTHERS = 3;

	public static final void filterRule(QueryNode query, RuleNode rule)
			throws Exception {
		final Rule r = rule.rule;
		if (r.type == 2) {
			final Rule2 r2 = (Rule2) r;
			if (r2.GENERICS_STRATS[0].canFilterBindingsFromHead != null
					&& r2.GENERICS_STRATS[0].canFilterBindingsFromHead[0]) {
				final int posInHead = r2.GENERICS_STRATS[0].posHeadFilterBindingsFromHead[0];
				if (posInHead == query.posFilterValues) {
					rule.idFilterValues = query.idFilterValues;
					rule.posFilterValues = r2.GENERICS_STRATS[0].posGenFilterBindingsFromHead[0];
					// Pass it to the child
					QueryNode childQuery = (QueryNode) rule.child;
					while (childQuery != null) {
						childQuery.idFilterValues = query.idFilterValues;
						childQuery.posFilterValues = rule.posFilterValues;
						childQuery = (QueryNode) childQuery.sibling;
					}
				}
			}
		} else if (r.type == 3
				|| (r.type == 4 && ((Rule2) r).GENERICS_STRATS != null)) {
			final Rule2 r2 = (Rule2) r;
			final int sg = rule.strag_id;
			if (r2.GENERICS_STRATS[sg].canFilterBindingsFromHead != null
					&& r2.GENERICS_STRATS[sg].canFilterBindingsFromHead[sg]) {
				final int posInHead = r2.GENERICS_STRATS[sg].posHeadFilterBindingsFromHead[sg];
				if (posInHead == query.posFilterValues) {
					rule.idFilterValues = query.idFilterValues;
					rule.posFilterValues = r2.GENERICS_STRATS[sg].posGenFilterBindingsFromHead[sg];
					// Pass it to the child
					QueryNode childQuery = (QueryNode) rule.child;
					while (childQuery != null) {
						childQuery.idFilterValues = query.idFilterValues;
						childQuery.posFilterValues = rule.posFilterValues;
						childQuery = (QueryNode) childQuery.sibling;
					}
				}
			}
		} else if (r.type == 4) {
			// Get it from the lists
			final Rule4 r4 = (Rule4) r;
			if (r4.canFilterBindingsFromHead) {
				final int posInHead = r4.posHeadFilterBindingsFromHead;
				if (posInHead == query.posFilterValues) {
					rule.idFilterValues = query.idFilterValues;
					rule.posFilterValues = r4.posGenFilterBindingsFromHead;
					// Pass it to the child
					QueryNode childQuery = (QueryNode) rule.child;
					while (childQuery != null) {
						childQuery.idFilterValues = query.idFilterValues;
						childQuery.posFilterValues = rule.posFilterValues;
						childQuery = (QueryNode) childQuery.sibling;
					}
				}
			}
		}
	}

	private static final void propagateFilterValues(QueryNode query)
			throws Exception {
		if (query.idFilterValues != -1) {
			RuleNode rule = (RuleNode) query.child;
			while (rule != null) {
				filterRule(query, rule);
				rule = (RuleNode) rule.sibling;
			}
		}
	}

	public static final void expandQuery(ActionContext context,
			QueryNode query, Tree tree, int typeRules,
			List<QueryNode> excludeQueries) throws Exception {
		// Check whether the subject has a special flag
		if (query.getS() == Schema.SCHEMA_SUBSET) {
			return;
		}

		if (query.equalsInAncestors(context)) {
			return;
		}

		if (typeRules == ONLY_FIRST_SECOND_RECORD_OTHERS
				&& !query.isNew(context)) {
			query.setExpanded();
			return;
		}

		final RuleNode existingRules = (RuleNode) query.child;
		RuleNode lastRule = null;
		final MultiValue k1 = new MultiValue(new long[1]);
		final MultiValue k2 = new MultiValue(new long[2]);

		if (typeRules != ONLY_THIRD_FOURTH) {
			for (final Rule1 rule : ruleset.getAllActiveFirstTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}
				final RuleNode c = applyRuleFirstType(rule, query, tree,
						excludeQueries);

				if (c != null) {
					if (lastRule == null) {
						query.child = c;
					} else {
						lastRule.sibling = c;
					}
					lastRule = c;
				}
			}

			for (final Rule2 rule : ruleset.getAllActiveSecondTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}
				final RuleNode c = applyRuleWithGenerics(rule,
						rule.GENERICS_STRATS, query, context, k1, k2, tree,
						excludeQueries);

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
			for (final Rule3 rule : ruleset.getAllActiveThirdTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}
				RuleNode c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
						query, context, k1, k2, tree, excludeQueries);

				if (c != null) {
					if (lastRule == null) {
						query.child = c;
					} else {
						lastRule.sibling = c;
					}
					do {
						lastRule = c;
						c = (RuleNode) c.sibling;
					} while (c != null);
				}
			}

			for (final Rule4 rule : ruleset.getAllActiveFourthTypeRules()) {
				if (!checkHead(rule, query, context)) {
					continue;
				}

				RuleNode c = null;
				if (rule.GENERICS_STRATS == null && rule.LIST_PATTERNS == null) {
					c = applyRuleFirstType(rule, query, tree, excludeQueries);
				} else if (rule.GENERICS_STRATS != null) {
					c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
							query, context, k1, k2, tree, excludeQueries);
				} else {
					c = applyRuleWithGenericsList(rule, query, context, k1,
							tree, excludeQueries);
				}

				if (c != null) {
					if (lastRule == null) {
						query.child = c;
					} else {
						lastRule.sibling = c;
					}
					do {
						lastRule = c;
						c = (RuleNode) c.sibling;
					} while (c != null);
				}
			}
		}

		if (lastRule != null) {
			lastRule.sibling = existingRules;
		}

		// Check if I can propagate the filterValues
		propagateFilterValues(query);
	}

	private static final RuleNode applyRuleFirstType(Rule1 rule,
			QueryNode head, Tree tree, List<QueryNode> excludedQueries)
			throws Exception {

		if (excludedQueries != null) {
			// Check if the rule is not the same
			for (QueryNode existingQuery : excludedQueries) {
				RuleNode parent = (RuleNode) existingQuery.parent;
				if (parent.rule.id == rule.id) {
					return null;
				}
			}
		}

		final RuleNode child = tree.newRule(head, rule);
		// Set the query
		final QueryNode childQuery = tree.newQuery(child);
		childQuery.setS(Schema.SCHEMA_SUBSET);
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
				t = query.getS();
				break;
			case 1:
				t = query.p;
				break;
			case 2:
				t = query.o;
				break;
			}

			final RDFTerm head_value = rule.HEAD.p[i];
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
			final GenericVars gv = generics[currentStrategy];

			int n_current_vars = gv.pos_variables_generic_patterns[currentStrategy].length;

			final Mapping[] pos_head = gv.pos_shared_vars_generics_head[0];
			if (pos_head != null)
				for (final Mapping map : pos_head) {
					long v = 0;
					switch (map.pos2) {
					case 0:
						v = head.getS();
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
		triple.setS(pattern.p[0].getValue());
		triple.p = pattern.p[1].getValue();
		triple.o = pattern.p[2].getValue();
	}

	public static final void unify(QueryNode input, QueryNode output,
			Mapping[] positions) {
		if (positions != null) {
			for (final Mapping pos : positions) {
				output.setTerm(pos.pos1, input.getTerm(pos.pos2));
			}
		}
	}

	private static final RuleNode applyRuleWithGenericsList(Rule4 rule,
			QueryNode head, ActionContext context, MultiValue k1, Tree tree,
			List<QueryNode> excludeQueries) throws Exception {

		if (rule.precomputed_patterns_head != null
				&& rule.precomputed_patterns_head.length == 2) {
			throw new Exception("Not supported");
		}

		// Unify the pattern with the variables that come from the precomputed
		// patterns
		final boolean firstOption = rule.precomputed_patterns_head.length == 0
				|| (rule.precomputed_patterns_head.length == 1 && head
						.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES);

		// Get the possible lists that is possible to follow.
		Collection<Long> list_heads = null;
		if (firstOption) {
			final RuleNode output = tree.newRule(head, rule);
			output.single_node = true;

			list_heads = rule.all_lists.keySet();

			final QueryNode query = tree.newQuery(output);
			query.list_heads = list_heads;
			query.list_id = 0;

			long shared_set_id = ((long) (context.getNewBucketID() * -1)) << 16;
			Collection<Long> els = RuleExecutor4.getElementsFromLists(
					rule.all_lists, list_heads, 0);
			context.putObjectInCache(shared_set_id, els);
			rule.substituteListNameValueInPattern(rule.LIST_PATTERNS[0], query,
					0, shared_set_id);
			unify(head, query, rule.lshared_var_firsthead[0]);

			if (notExisting(excludeQueries, output, query, tree, context)) {
				output.child = query;
				return output;
			} else {
				QueryNode removedQuery = tree.removeLastQuery();
				assert (query == removedQuery);
				tree.removeRule(output);
				return null;
			}
		} else {
			k1.values[0] = head.getTerm(rule.precomputed_patterns_head[0].pos2);
			final CollectionTuples col = rule.mapping_head_list_heads.get(k1);

			if (col != null) {
				RuleNode firstRule = null;
				RuleNode lastRule = null;

				for (int i = col.getStart(); i < col.getEnd(); ++i) {
					final long list_head = col.getRawValues()[i];
					final List<Long> list = rule.all_lists.get(list_head);
					if (list != null) {
						final RuleNode output = tree.newRule(head, rule);
						output.single_node = true;
						final QueryNode query = tree.newQuery(output);
						query.list_heads = new HashSet<Long>();
						query.list_heads.add(list_head);
						query.list_id = 0;

						rule.substituteListNameValueInPattern(
								rule.LIST_PATTERNS[0], query, 0, list.get(0));
						unify(head, query, rule.lshared_var_firsthead[0]);

						if (!notExisting(excludeQueries, output, query, tree,
								context)) {
							// Remove the query and do not add it
							QueryNode removedQuery = tree.removeLastQuery();
							assert (query == removedQuery);
							tree.removeRule(output);
						} else {
							output.child = query;
							if (firstRule == null) {
								firstRule = output;
							}
							if (lastRule != null) {
								lastRule.sibling = output;
							}
							lastRule = output;
						}
					}
				}
				return firstRule;
			}
			return null;
		}
	}

	private static final RuleNode applyRuleWithGenerics(Rule1 rule,
			GenericVars[] generics, QueryNode head, ActionContext context,
			MultiValue k1, MultiValue k2, Tree tree,
			List<QueryNode> excludeQueries) throws Exception {
		// First determine what is the best strategy to execute the patterns
		// given a instantiated HEAD
		final int strategy = calculate_best_strategy(generics, head);
		final GenericVars g = generics[strategy];
		return applyRuleWithGenerics(rule, strategy, g, g.patterns[0], -1,
				head, context, k1, k2, tree, excludeQueries);

	}

	private static final boolean notExisting(
			final List<QueryNode> excludeQueries, final RuleNode rule,
			final QueryNode query, final Tree tree, final ActionContext context) {
		if (excludeQueries != null) {
			for (QueryNode existingQuery : excludeQueries) {
				RuleNode parent = (RuleNode) existingQuery.parent;
				if (parent.equalsTo(rule)
						&& query.isContainedIn(existingQuery, context)) {
					return false;
				}
			}
		}
		return true;
	}

	private static final RuleNode expandFirstOption(Rule1 rule, int strategy,
			GenericVars g, Pattern p, int list_id, QueryNode instantiated_head,
			ActionContext context, MultiValue k1, MultiValue k2, Tree tree,
			List<QueryNode> excludeQueries) {
		final RuleNode output = tree.newRule(instantiated_head, rule);
		output.strag_id = strategy;
		output.single_node = rule.type != 2;
		final QueryNode tripleOutput = tree.newQuery(output);
		tripleOutput.list_id = list_id;

		// Unify tripleOutput with the values of the GENERIC PATTERN
		unify(p, tripleOutput);

		// Unify the triple with the values from the head that are not used
		// anywhere else
		unify(instantiated_head, tripleOutput,
				g.pos_shared_vars_generics_head[0]);

		if (g.pos_shared_vars_precomp_generics[0].length > 0) {
			tripleOutput.setTerm(g.pos_shared_vars_precomp_generics[0][0].pos2,
					g.id_all_values_first_generic_pattern);
		}

		if (g.pos_shared_vars_precomp_generics[0].length > 1) {
			tripleOutput.setTerm(g.pos_shared_vars_precomp_generics[0][1].pos2,
					g.id_all_values_first_generic_pattern2);
		}

		if (notExisting(excludeQueries, output, tripleOutput, tree, context)) {
			output.child = tripleOutput;
			return output;
		} else {
			QueryNode removedQuery = tree.removeLastQuery();
			assert (tripleOutput == removedQuery);
			tree.removeRule(output);
			return null;
		}
	}

	private static final RuleNode expandOneSharedVar(Rule1 rule, int strategy,
			GenericVars g, Pattern p, int list_id, QueryNode instantiated_head,
			ActionContext context, MultiValue k1, MultiValue k2, Tree tree,
			List<QueryNode> excludeQueries) {
		final long v = instantiated_head
				.getTerm(rule.precomputed_patterns_head[0].pos2);
		Collection<Long> possibleKeys = null;
		if (v >= 0) {
			possibleKeys = new ArrayList<Long>();
			possibleKeys.add(v);
		} else {
			possibleKeys = schema.getSubset(v, context);
		}

		RuleNode firstRuleNode = null;
		RuleNode lastRuleNode = null;
		RuleNode ruleNode = null;
		if (rule.type == 2) {
			ruleNode = tree.newRule(instantiated_head, rule);
			ruleNode.strag_id = strategy;
			ruleNode.single_node = false;
			lastRuleNode = firstRuleNode = ruleNode;
		}
		QueryNode lastQuery = null;
		for (final long key : possibleKeys) {
			k1.values[0] = key;
			final CollectionTuples possibleBindings = g.mapping_head_first_generic
					.get(k1);
			if (possibleBindings != null) {
				for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
					if (g.filter_values_last_generic_pattern
							&& v == possibleBindings.getValue(y, 0)) {
						continue;
					}

					if (rule.type != 2) {
						ruleNode = tree.newRule(instantiated_head, rule);
						ruleNode.strag_id = strategy;
						ruleNode.single_node = true;
					}

					final QueryNode newQuery = tree.newQuery(ruleNode);
					newQuery.list_id = list_id;
					if (ruleNode.child == null) {
						unify(p, newQuery);
						unify(instantiated_head, newQuery,
								g.pos_shared_vars_generics_head[0]);
					} else {
						// Copy old values
						newQuery.setS(lastQuery.getS());
						newQuery.p = lastQuery.p;
						newQuery.o = lastQuery.o;
					}

					for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
						newQuery.setTerm(
								g.pos_shared_vars_precomp_generics[0][i].pos2,
								possibleBindings.getValue(y, i));
					}

					if (!notExisting(excludeQueries, ruleNode, newQuery, tree,
							context)) {
						// Remove the query and do not add it
						QueryNode removedQuery = tree.removeLastQuery();
						assert (newQuery == removedQuery);
						if (rule.type != 2) {
							tree.removeRule(ruleNode);
						}
					} else {
						if (ruleNode.child == null) {
							ruleNode.child = newQuery;
						} else {
							lastQuery.sibling = newQuery;
						}
						lastQuery = newQuery;

						if (rule.type != 2) {
							if (firstRuleNode == null) {
								firstRuleNode = ruleNode;
							} else {
								lastRuleNode.sibling = ruleNode;
							}
							lastRuleNode = ruleNode;
						}
					}
				}
			}
		}

		if (firstRuleNode == null) {
			return null;
		} else if (firstRuleNode.child == null) {
			tree.removeRule(firstRuleNode);
			return null;
		} else { // Everything is ok
			return firstRuleNode;
		}
	}

	private static final RuleNode expandTwoSharedVarsBothConstants(Rule1 rule,
			int strategy, GenericVars g, Pattern p, int list_id,
			QueryNode instantiated_head, ActionContext context, MultiValue k1,
			MultiValue k2, Tree tree, List<QueryNode> excludeQueries) {
		// Use both values to restrict the search
		k2.values[0] = instantiated_head
				.getTerm(rule.precomputed_patterns_head[0].pos2);
		k2.values[1] = instantiated_head
				.getTerm(rule.precomputed_patterns_head[1].pos2);
		final CollectionTuples possibleBindings = g.mapping_head_first_generic
				.get(k2);
		if (possibleBindings != null) {

			RuleNode firstRuleNode = null;
			RuleNode lastRuleNode = null;
			RuleNode ruleNode = null;
			if (rule.type == 2) {
				ruleNode = tree.newRule(instantiated_head, rule);
				ruleNode.strag_id = strategy;
				ruleNode.single_node = false;
				lastRuleNode = firstRuleNode = ruleNode;
			}

			QueryNode lastQuery = null;
			for (int y = 0; y < possibleBindings.getNTuples(); ++y) {

				if (rule.type != 2) {
					ruleNode = tree.newRule(instantiated_head, rule);
					ruleNode.strag_id = strategy;
					ruleNode.single_node = true;
				}

				final QueryNode newQuery = tree.newQuery(ruleNode);
				if (ruleNode.child == null) {
					newQuery.list_id = list_id;
					unify(p, newQuery);
					unify(instantiated_head, newQuery,
							g.pos_shared_vars_generics_head[0]);
				} else {
					newQuery.list_id = list_id;
					lastQuery.sibling = newQuery;
					newQuery.setS(lastQuery.getS());
					newQuery.p = lastQuery.p;
					newQuery.o = lastQuery.o;
				}

				for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
					newQuery.setTerm(
							g.pos_shared_vars_precomp_generics[0][i].pos2,
							possibleBindings.getValue(y, i));
				}

				if (!notExisting(excludeQueries, ruleNode, newQuery, tree,
						context)) {
					// Remove the query and do not add it
					QueryNode removedQuery = tree.removeLastQuery();
					assert (newQuery == removedQuery);
					if (rule.type != 2) {
						tree.removeRule(ruleNode);
					}
				} else {
					if (ruleNode.child == null) {
						ruleNode.child = newQuery;
					} else {
						lastQuery.sibling = newQuery;
					}
					lastQuery = newQuery;

					if (rule.type != 2) {
						if (firstRuleNode == null) {
							firstRuleNode = ruleNode;
						} else {
							lastRuleNode.sibling = ruleNode;
						}
						lastRuleNode = ruleNode;
					}
				}
			}
			if (firstRuleNode == null) {
				return null;
			} else if (firstRuleNode.child == null) {
				tree.removeRule(firstRuleNode);
				return null;
			}
			return firstRuleNode;
		}
		return null;
	}

	private static final RuleNode expandTwoSharedVarsNoConstants1(Rule1 rule,
			int strategy, GenericVars g, Pattern p, int list_id,
			QueryNode instantiated_head, ActionContext context, MultiValue k1,
			MultiValue k2, Tree tree, List<QueryNode> excludeQueries)
					throws Exception {
		CollectionTuples possibleBindings;
		final long idSet = instantiated_head
				.getTerm(rule.precomputed_patterns_head[0].pos2);
		final Collection<Long> col = schema.getSubset(idSet, context);
		if (col != null) {
			final TreeSet<Long> o = new TreeSet<Long>();

			if (g.pos_shared_vars_precomp_generics[0].length > 1) {
				throw new Exception("Not implemented");
			}

			for (final long possibleValue : col) {
				k1.values[0] = possibleValue;
				possibleBindings = g.mapping_head1_first_generic.get(k1);
				if (possibleBindings != null) {
					for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
						o.add(possibleBindings.getValue(y, 0));
					}
				}
			}

			if (o.size() > 0) {
				final long shared_set_id = ((long) (context.getNewBucketID() * -1)) << 16;
				context.putObjectInCache(shared_set_id, o);
				context.broadcastCacheObjects(shared_set_id);

				final RuleNode output = tree.newRule(instantiated_head, rule);
				output.strag_id = strategy;
				output.single_node = rule.type != 2;
				final QueryNode supportTriple = tree.newQuery(output);
				supportTriple.list_id = list_id;
				unify(p, supportTriple);
				unify(instantiated_head, supportTriple,
						g.pos_shared_vars_generics_head[0]);
				supportTriple.setTerm(
						g.pos_shared_vars_precomp_generics[0][0].pos2,
						shared_set_id);

				if (!notExisting(excludeQueries, output, supportTriple, tree,
						context)) {
					// Remove the query and do not add it
					QueryNode removedQuery = tree.removeLastQuery();
					assert (supportTriple == removedQuery);
					tree.removeRule(output);
					return null;
				} else {
					output.child = supportTriple;
					return output;
				}
			}
		}
		return null;
	}

	private static final RuleNode expandTwoSharedVarsNoConstants2(Rule1 rule,
			int strategy, GenericVars g, Pattern p, int list_id,
			QueryNode instantiated_head, ActionContext context, MultiValue k1,
			MultiValue k2, Tree tree, List<QueryNode> excludeQueries)
			throws Exception {
		final long idSet = instantiated_head
				.getTerm(rule.precomputed_patterns_head[1].pos2);
		CollectionTuples possibleBindings;

		final Collection<Long> col = schema.getSubset(idSet, context);
		if (col != null) {

			final TreeSet<Long> o = new TreeSet<Long>();

			if (g.pos_shared_vars_precomp_generics[0].length > 1) {
				throw new Exception("Not implemented");
			}

			for (final long possibleValue : col) {
				k1.values[0] = possibleValue;
				possibleBindings = g.mapping_head2_first_generic.get(k1);
				if (possibleBindings != null) {
					for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
						o.add(possibleBindings.getValue(y, 0));
					}
				}
			}

			if (o.size() > 0) {
				final RuleNode output = tree.newRule(instantiated_head, rule);
				final long shared_set_id = ((long) (context.getNewBucketID() * -1)) << 16;
				context.putObjectInCache(shared_set_id, o);
				context.broadcastCacheObjects(shared_set_id);
				final QueryNode supportTriple = tree.newQuery(output);
				supportTriple.list_id = list_id;
				unify(p, supportTriple);
				unify(instantiated_head, supportTriple,
						g.pos_shared_vars_generics_head[0]);
				supportTriple.setTerm(
						g.pos_shared_vars_precomp_generics[0][0].pos2,
						shared_set_id);

				if (!notExisting(excludeQueries, output, supportTriple, tree,
						context)) {
					// Remove the query and do not add it
					QueryNode removedQuery = tree.removeLastQuery();
					assert (supportTriple == removedQuery);
					tree.removeRule(output);
					return null;
				} else {
					output.child = supportTriple;
					return output;
				}
			}
		}
		return null;
	}

	private static final RuleNode expandTwoSharedVarsNoConstants3(Rule1 rule,
			int strategy, GenericVars g, Pattern p, int list_id,
			QueryNode instantiated_head, ActionContext context, MultiValue k1,
			MultiValue k2, Tree tree, List<QueryNode> excludeQueries) {
		final Collection<Long> col1 = Schema
				.getInstance()
				.getSubset(
						instantiated_head
								.getTerm(rule.precomputed_patterns_head[0].pos2),
						context);
		final Collection<Long> col2 = Schema
				.getInstance()
				.getSubset(
						instantiated_head
								.getTerm(rule.precomputed_patterns_head[1].pos2),
						context);
		if (col1 != null && col2 != null) {
			// Calculate all the set of keys.
			RuleNode firstRuleNode = null;
			RuleNode lastRuleNode = null;
			RuleNode ruleNode = null;
			if (rule.type == 2) {
				ruleNode = tree.newRule(instantiated_head, rule);
				ruleNode.strag_id = strategy;
				ruleNode.single_node = false;
				lastRuleNode = firstRuleNode = ruleNode;
			}
			QueryNode lastQuery = null;
			for (final long value1 : col1) {
				for (final long value2 : col2) {
					k2.values[0] = value1;
					k2.values[1] = value2;
					final CollectionTuples col = g.mapping_head3_first_generic
							.get(k2);
					if (col != null) {
						final int sizeTuple = col.getSizeTuple();
						for (int i = col.getStart(); i < col.getEnd(); i += sizeTuple) {
							if (rule.type != 2) {
								ruleNode = tree
										.newRule(instantiated_head, rule);
								ruleNode.strag_id = strategy;
								ruleNode.single_node = true;
							}
							final QueryNode newQuery = tree.newQuery(ruleNode);
							if (ruleNode.child == null) {
								newQuery.list_id = list_id;
								unify(p, newQuery);
								unify(instantiated_head, newQuery,
										g.pos_shared_vars_generics_head[0]);
							} else {
								newQuery.list_id = list_id;
								newQuery.setS(lastQuery.getS());
								newQuery.p = lastQuery.p;
								newQuery.o = lastQuery.o;
							}
							for (int j = 0; j < sizeTuple; ++j) {
								newQuery.setTerm(
										g.pos_shared_vars_precomp_generics[0][j].pos2,
										col.getRawValues()[i]);
							}

							if (!notExisting(excludeQueries, ruleNode,
									newQuery, tree, context)) {
								QueryNode removedQuery = tree.removeLastQuery();
								assert (newQuery == removedQuery);
								if (rule.type != 2) {
									tree.removeRule(ruleNode);
								}
							} else {
								if (ruleNode.child == null) {
									ruleNode.child = newQuery;
								} else {
									lastQuery.sibling = newQuery;
								}
								lastQuery = newQuery;
								if (rule.type != 2) {
									if (firstRuleNode == null) {
										firstRuleNode = ruleNode;
									} else {
										lastRuleNode.sibling = ruleNode;
									}
									lastRuleNode = ruleNode;
								}
							}
						}
					}
				}
			}

			if (firstRuleNode == null) {
				return null;
			} else if (firstRuleNode.child == null) {
				tree.removeRule(firstRuleNode);
				return null;
			}
			return firstRuleNode;
		}
		return null;
	}

	private static final RuleNode expandTwoSharedVarsNoConstants4(
			CollectionTuples possibleBindings, Rule1 rule, int strategy,
			GenericVars g, Pattern p, int list_id, QueryNode instantiated_head,
			ActionContext context, MultiValue k1, MultiValue k2, Tree tree,
			List<QueryNode> excludeQueries) {
		RuleNode firstRuleNode = null;
		RuleNode lastRuleNode = null;
		RuleNode ruleNode = null;
		if (rule.type == 2) {
			ruleNode = tree.newRule(instantiated_head, rule);
			ruleNode.strag_id = strategy;
			ruleNode.single_node = false;
			lastRuleNode = firstRuleNode = ruleNode;
		}

		if (possibleBindings != null) {
			QueryNode lastQuery = null;
			for (int y = 0; y < possibleBindings.getNTuples(); ++y) {

				if (rule.type != 2) {
					ruleNode = tree.newRule(instantiated_head, rule);
					ruleNode.strag_id = strategy;
					ruleNode.single_node = true;
				}

				final QueryNode newQuery = tree.newQuery(ruleNode);
				if (ruleNode.child == null) {
					newQuery.list_id = list_id;
					unify(p, newQuery);
					unify(instantiated_head, newQuery,
							g.pos_shared_vars_generics_head[0]);
				} else {
					newQuery.list_id = list_id;
					newQuery.setS(lastQuery.getS());
					newQuery.p = lastQuery.p;
					newQuery.o = lastQuery.o;
				}

				for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
					newQuery.setTerm(
							g.pos_shared_vars_precomp_generics[0][i].pos2,
							possibleBindings.getValue(y, i));
				}

				if (!notExisting(excludeQueries, ruleNode, newQuery, tree,
						context)) {
					QueryNode removedQuery = tree.removeLastQuery();
					assert (newQuery == removedQuery);
					if (rule.type != 2) {
						tree.removeRule(ruleNode);
					}
				} else {
					if (ruleNode.child == null) {
						ruleNode.child = newQuery;
					} else {
						lastQuery.sibling = newQuery;
					}
					lastQuery = newQuery;

					if (rule.type != 2) {
						if (firstRuleNode == null) {
							firstRuleNode = ruleNode;
						} else {
							lastRuleNode.sibling = ruleNode;
						}
						lastRuleNode = ruleNode;
					}
				}
			}
			if (firstRuleNode == null) {
				return null;
			} else if (firstRuleNode.child == null) {
				tree.removeRule(firstRuleNode);
				return null;
			}
			return firstRuleNode;
		}
		return null;
	}

	private static final RuleNode applyRuleWithGenerics(Rule1 rule,
			int strategy, GenericVars g, Pattern p, int list_id,
			QueryNode instantiated_head, ActionContext context, MultiValue k1,
			MultiValue k2, Tree tree, List<QueryNode> excludeQueries)
			throws Exception {
		// Unify the pattern with the variables that come from the precomputed
		// patterns
		final boolean firstOption = rule.precomputed_tuples == null
				|| rule.precomputed_patterns_head.length == 0
				|| (rule.precomputed_patterns_head.length == 1 && instantiated_head
						.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES)
				|| (rule.precomputed_patterns_head.length == 2
						&& instantiated_head
								.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES && instantiated_head
						.getTerm(rule.precomputed_patterns_head[1].pos2) == Schema.ALL_RESOURCES);

		if (firstOption) {
			return expandFirstOption(rule, strategy, g, p, list_id,
					instantiated_head, context, k1, k2, tree, excludeQueries);
		} else {
			if (rule.precomputed_patterns_head.length == 1) {
				return expandOneSharedVar(rule, strategy, g, p, list_id,
						instantiated_head, context, k1, k2, tree,
						excludeQueries);
			} else if (rule.precomputed_patterns_head.length == 2) {
				if (instantiated_head
						.getTerm(rule.precomputed_patterns_head[0].pos2) >= 0
						&& instantiated_head
								.getTerm(rule.precomputed_patterns_head[1].pos2) >= 0) {
					return expandTwoSharedVarsBothConstants(rule, strategy, g,
							p, list_id, instantiated_head, context, k1, k2,
							tree, excludeQueries);
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
							return expandTwoSharedVarsNoConstants1(rule,
									strategy, g, p, list_id, instantiated_head,
									context, k1, k2, tree, excludeQueries);
						} else if (instantiated_head
								.getTerm(rule.precomputed_patterns_head[1].pos2) <= Schema.SET_THRESHOLD
								&& instantiated_head
										.getTerm(rule.precomputed_patterns_head[0].pos2) == Schema.ALL_RESOURCES) {
							return expandTwoSharedVarsNoConstants2(rule,
									strategy, g, p, list_id, instantiated_head,
									context, k1, k2, tree, excludeQueries);
						} else {
							return expandTwoSharedVarsNoConstants3(rule,
									strategy, g, p, list_id, instantiated_head,
									context, k1, k2, tree, excludeQueries);
						}
					}

					return expandTwoSharedVarsNoConstants4(possibleBindings,
							rule, strategy, g, p, list_id, instantiated_head,
							context, k1, k2, tree, excludeQueries);
				}
			} else {
				throw new Exception("Not supported");
			}
		}
	}

	public static void reExpandQuery(ActionContext context, QueryNode q,
			Tree t, int typeRules) throws Exception {
		RuleNode existingRules = (RuleNode) q.child;
		// Get all queries
		List<QueryNode> excludeQueries = new ArrayList<>();
		while (existingRules != null) {
			QueryNode childQuery = (QueryNode) existingRules.child;
			while (childQuery != null) {
				excludeQueries.add(childQuery);
				childQuery = (QueryNode) childQuery.sibling;
			}
			existingRules = (RuleNode) existingRules.sibling;
		}
		TreeExpander.expandQuery(context, q, t, typeRules, excludeQueries);
	}
}
