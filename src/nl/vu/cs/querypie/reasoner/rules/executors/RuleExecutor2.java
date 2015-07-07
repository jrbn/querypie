package nl.vu.cs.querypie.reasoner.rules.executors;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.Rule2.GenericVars;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.MultiValue;
import nl.vu.cs.querypie.storage.memory.TupleMap;
import nl.vu.cs.querypie.storage.memory.TupleSet;
import nl.vu.cs.querypie.storage.memory.tuplewrapper.RDFTermTupleWrapper;
import nl.vu.cs.querypie.storage.memory.tuplewrapper.SimpleDataTupleWrapper;
import nl.vu.cs.querypie.storage.memory.tuplewrapper.TupleWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleExecutor2 extends RuleExecutor1 {

	public static final int I_FILTERVALUESSET = 5;

	static final Logger log = LoggerFactory.getLogger(RuleExecutor2.class);

	// Strategy to execute the generic patterns
	private Rule2 ruleDef;
	protected GenericVars g;
	protected Set<Long> filterValues = null;
	protected int posToFilter;

	/***** DATA STRUCTURES USED FOR STRATIFICATION *****/
	// protected RDFTerm[] triple2 = { new RDFTerm(), new RDFTerm(), new
	// RDFTerm() };
	// private boolean first;
	// private InMemoryTripleContainer triples_derived_prev_cycle = null;
	// private boolean check_stratification;

	/***** DATA STRUCTURES USED FOR THE JOIN *****/
	// Used to perform the join
	private TupleMap mapping_last_generic_head;
	private Mapping[] shared_vars_precomps_last_generic;
	// Number of variables in the instantiated pattern that are retrieved from
	// the generic pattern
	private int n_vars_gen_output;
	private final Mapping[] vars_gen_output = new Mapping[3];
	// Number of variables in the instantiated pattern that are retrieved from
	// the precalculated triples
	private int n_vars_others_output;
	private final Mapping[] vars_others_output = new Mapping[3];
	private boolean should_redo_join;

	protected SimpleDataTupleWrapper tw = new SimpleDataTupleWrapper();
	private final RDFTermTupleWrapper wrapper = new RDFTermTupleWrapper();

	/***** DATA STRUCTURES USED FOR REMOVE THE DUPLICATES *****/
	private MultiValue key;
	private final MultiValue k3 = new MultiValue(new long[3]);
	protected Set<MultiValue> duplicates = new HashSet<MultiValue>();

	private TupleMap prepareMappingsForLastJoin(Pattern HEAD,
			TupleSet actual_precomputed_tuples, boolean compress,
			ActionContext context) throws Exception {

		// Determine in the head where the remaining variables come from
		n_vars_others_output = n_vars_gen_output = 0;
		for (int i = 0; i < 3; ++i) {

			long v = instantiated_head[i].getValue();
			if (HEAD.p[i].getValue() >= 0) {
				triple[i].setValue(HEAD.p[i].getValue());
			} else {
				triple[i].setValue(v);
			}

			if (v < 0) {
				Mapping[] mappings = g.pos_shared_vars_generics_head[g.patterns.length - 1];
				boolean found = false;
				if (mappings != null) {
					for (Mapping mapping : mappings) {
						if (mapping.pos2 == i) {
							found = true;
							vars_gen_output[n_vars_gen_output++] = mapping;
							break;
						}
					}
				}
				if (!found) {
					// Must find it within the precomputed patterns
					List<String> list = actual_precomputed_tuples
							.getNameBindings();
					int j = 0;
					for (String l : list) {
						if (l.equals(HEAD.p[i].getName())) {
							Mapping m = new Mapping();
							m.pos1 = j;
							m.pos2 = i;
							m.nameBinding = l;
							vars_others_output[n_vars_others_output++] = m;
							break;
						}
						j++;
					}
				}
			}
		}

		if (n_vars_gen_output == 3) {
			throw new Exception("Not supported");
		}

		if (n_vars_others_output > 0 && n_vars_others_output < 3) {
			shared_vars_precomps_last_generic = g.pos_shared_vars_precomp_generics[g.patterns.length - 1];

			int[] poss = new int[shared_vars_precomps_last_generic.length];
			for (int i = 0; i < poss.length; ++i) {
				poss[i] = shared_vars_precomps_last_generic[i].pos1;
			}

			int[] output = new int[n_vars_others_output];

			for (int i = 0; i < output.length; ++i) {
				output[i] = vars_others_output[i].pos1;
			}
			return actual_precomputed_tuples.getBindingsFromBindings(poss,
					output, compress);
		} else if (n_vars_others_output == 3) {
			// } else {
			throw new Exception("Not supported");
		}

		return null;
	}

	protected void prepareForJoin(Rule2 rule, TupleSet precomputed_set,
			GenericVars g, RDFTerm[] instantiated_head, boolean compress,
			ActionContext context) throws Exception {
		// first = true;
		for (int i = 0; i < 3; ++i)
			k3.values[i] = instantiated_head[i].getValue();

		mapping_last_generic_head = prepareMappingsForLastJoin(rule.HEAD,
				precomputed_set, compress, context);

		if (shared_vars_precomps_last_generic != null
				&& shared_vars_precomps_last_generic.length == 1) {
			key = k1;
		} else {
			key = k2;
		}

		// Recursion
		int n_recursive_patterns = 0;
		int[] pos_recursive_patterns = new int[5];

		if (g.head_used_generic_pattern) {
			for (int i = 0; i < g.are_generic_patterns_recursive.length; ++i) {
				if (g.are_generic_patterns_recursive[i] != null) {
					boolean ok = true;
					for (int j = 0; j < 3; ++j) {
						RDFTerm t = instantiated_head[j];
						if (t.getValue() >= 0) {
							int pos = g.are_generic_patterns_recursive[i][j];
							if (pos != -1) {
								Collection<Long> col = rule.precomputed_tuples
										.getAllValues(pos, true);
								if (col != null && !col.contains(t.getValue())) {
									ok = false;
									break;
								}
							}
						}
					}

					if (ok && i > 0) {
						// This is second or higher generic pattern. The
						// variable that the pattern has shared with the
						// previous point might conflict with the value of the
						// instantiated point and this would make the pattern
						// not recursive.
						Mapping map = g.shared_var_with_next_gen_pattern[i - 1];
						int pos = map.pos2;
						if (instantiated_head[pos].getValue() >= 0) {
							Collection<Long> col = null;
							if (precomputed_set == rule.precomputed_tuples)
								col = precomputed_set
								.getAllValues(
										g.patterns[i - 1].p[map.pos1]
												.getName(), true);
							else
								col = precomputed_set
								.getAllValues(
										g.patterns[i - 1].p[map.pos1]
												.getName(), false);

							if (col.size() > 1
									|| !col.contains(instantiated_head[pos]
											.getValue())) {
								ok = false;
							}
						} else if (instantiated_head[pos].getValue() <= Schema.SET_THRESHOLD) {
							Collection<Long> col = null;
							if (precomputed_set == rule.precomputed_tuples)
								col = precomputed_set
								.getAllValues(
										g.patterns[i - 1].p[map.pos1]
												.getName(), true);
							else
								col = precomputed_set
								.getAllValues(
										g.patterns[i - 1].p[map.pos1]
												.getName(), false);

							Collection<Long> possibleValues = Schema
									.getInstance().getSubset(
											instantiated_head[pos].getValue(),
											context);
							for (long v : col) {
								if (!possibleValues.contains(v)) {
									ok = false;
									break;
								}
							}
						}
					}

					if (ok) {
						pos_recursive_patterns[n_recursive_patterns++] = i;
					}
				}
			}
		}

		should_redo_join = false;
		if (n_recursive_patterns > 0) {
			// If the recursive pattern is the last generic one, then we can
			// repeat the join using the output triple.
			if (n_recursive_patterns == 1) {

				// If the pattern derived is the recursive one then we can
				// simply rejoin the pattern
				if (pos_recursive_patterns[n_recursive_patterns - 1] == g.patterns.length - 1) {
					should_redo_join = true;
				}
			} else {
				// // We need to launch a new chain looking for new values
				// if (pos_recursive_patterns[n_recursive_patterns - 1] != 0) {
				// throw new Exception("Not implemented");
				// }

			}
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		super.registerActionParameters(conf);
		conf.registerParameter(I_FILTERVALUESSET, "I_FILTERVALUESSET", -1,
				false);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		// Get rule definition
		ruleDef = ruleset.getRuleSecondType(getParamInt(I_RULEDEF));

		// Instantiate the output
		for (int i = 0; i < 3; ++i) {
			instantiated_head[i].setValue(getParamLong(i));
			triple[i].setValue(ruleDef.HEAD.p[i].getValue());
		}

		g = ruleDef.GENERICS_STRATS[0];

		// Get the rule
		if (g.canFilterBindingsFromHead[0]) {
			int idValues = getParamInt(I_FILTERVALUESSET);
			if (idValues != -1) {
				filterValues = (Set<Long>) context
						.getObjectFromCache("filterValues-" + idValues);
				posToFilter = g.posGenFilterBindingsFromHead[0];
			}
		} else {
			filterValues = null;
		}

		prepareForJoin(ruleDef, ruleDef.precomputed_tuples, g,
				instantiated_head, true, context);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (filterValues != null
				&& filterValues
				.contains(((RDFTerm) inputTuple.get(posToFilter))
						.getValue())) {
			return;
		}
		tw.tuple = inputTuple;
		duplicates.clear();
		doJoin(tw, output, duplicates);
		duplicates.clear();
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		ruleDef = null;
		g = null;
		filterValues = null;
	}

	protected void doJoin(TupleWrapper t, ActionOutput output,
			Set<MultiValue> existingValues) throws Exception {

		for (int i = 0; i < n_vars_gen_output; ++i) {
			Mapping map = vars_gen_output[i];
			long v = t.get(map.pos1);
			triple[map.pos2].setValue(v);
			k3.values[map.pos2] = v;
		}

		if (n_vars_others_output > 0) {
			final int lengthFieldsToJoin = shared_vars_precomps_last_generic.length;
			final int lengthFieldsToCopy = n_vars_others_output;
			assert (lengthFieldsToJoin < 3);
			key.values[0] = t.get(shared_vars_precomps_last_generic[0].pos2);
			if (lengthFieldsToJoin == 2) {
				key.values[1] = t
						.get(shared_vars_precomps_last_generic[1].pos2);
			}

			CollectionTuples col = mapping_last_generic_head.get(key);
			if (col != null) {
				int pos1 = vars_others_output[0].pos2;
				int pos2 = -1;
				if (lengthFieldsToCopy == 2) {
					pos2 = vars_others_output[1].pos2;
				}

				for (int y = 0; y < col.getNTuples(); ++y) {
					k3.values[pos1] = col.getValue(y, 0);
					if (lengthFieldsToCopy == 2) {
						k3.values[pos2] = col.getValue(y, 1);
					}
					if (existingValues.contains(k3)) {
						continue;
					} else {
						existingValues.add(k3.newCopy());
					}

					triple[pos1].setValue(col.getValue(y, 0));
					if (lengthFieldsToCopy == 2) {
						triple[pos2].setValue(col.getValue(y, 1));
					}

					// if (triple[1].getValue() == -1) {
					// return;
					// }

					outputTuple(triple, output);

					if (should_redo_join) {
						wrapper.values = triple;
						doJoin(wrapper, output, existingValues);
					}
				}
			}
		} else {
			if (existingValues != null) {
				if (existingValues.contains(k3)) {
					return;
				}
				existingValues.add(k3.newCopy());
			}

			outputTuple(triple, output);
		}
	}

}