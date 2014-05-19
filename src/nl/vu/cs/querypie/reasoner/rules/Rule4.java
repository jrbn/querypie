package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoning.expand.QueryNode;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.MultiValue;
import nl.vu.cs.querypie.storage.memory.TupleMap;
import nl.vu.cs.querypie.storage.memory.TupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule4 extends Rule3 {

	static final Logger log = LoggerFactory.getLogger(Rule4.class);

	// Datastructures between the list and the head
	public int pos_head_list_in_precomps;
	public Map<Long, List<Long>> all_lists;
	public Mapping[] last_generic_first_list_mapping;

	public Pattern[] LIST_PATTERNS;

	public Mapping[][] lshared_var_head;
	public Mapping[][] lshared_var_firsthead;
	public Mapping[][] lshared_var_lasthead;

	public Mapping[][] lshared_var_with_next_gen_pattern;
	public Mapping[] lshared_var_new_list;
	public int[][] pos_unique_vars; // Used the first time
	public int[][] pos_unique_vars_next; // Used the second and consequent
	// times

	public int pos_list_in_head;
	public TupleMap mapping_head_list_heads;

	public int[] pos_vars_head;

	// public int n_vars_head_from_precomps;
	// public int[] pos_vars_head_from_precomps = new int[3];

	public Rule4(int id, String head, String[] precomps, String[] locations,
			String list_head, String list_loc, String[] generics)
			throws Exception {
		super(id, head, precomps, locations);
		type = 4;

		if (!isActive)
			return;

		pos_vars_head = HEAD.getPositionVars();

		// Load all the lists from the knowledge base
		all_lists = Schema.getInstance().getAllList(list_loc);
		if (all_lists == null || all_lists.size() == 0) {
			isActive = false;
			return;
		}

		// Match the head of the list against the precomp patterns.
		boolean found = false;
		pos_head_list_in_precomps = 0;
		for (String var : precomputed_tuples.getNameBindings()) {
			if (var.equals(list_head)) {
				found = true;
				break;
			}
			pos_head_list_in_precomps++;
		}
		if (!found) {
			throw new Exception("The precomp patterns do not match the list");
		}

		// Join the precomp. patterns against the list
		precomputed_tuples = precomputed_tuples.filter(
				pos_head_list_in_precomps, all_lists.keySet());
		if (precomputed_tuples.size() == 0) {
			isActive = false;
			return;
		}

		// Process the generics. Divide them between the ones that do not need
		// the lists and the others.
		List<Pattern> listPatterns = new ArrayList<Pattern>();
		List<Pattern> genPatterns = new ArrayList<Pattern>();

		if (generics != null) {
			Pattern[] patterns = parse_patterns(generics, null);
			for (Pattern pattern : patterns) {
				// If pattern contains list_el then it refers to lists
				found = false;
				for (RDFTerm t : pattern.p) {
					if (t.getName() != null && t.getName().equals("el_n")) {
						found = true;
						break;
					}
				}
				if (found) {
					listPatterns.add(pattern);
				} else {
					genPatterns.add(pattern);
				}
			}
			// Copy the list patterns in the global variable
			LIST_PATTERNS = listPatterns.toArray(new Pattern[listPatterns
					.size()]);
		}

		if (LIST_PATTERNS == null || LIST_PATTERNS.length == 0) {
			// Reconstruct the precomputed tuples replacing the HEAD with the
			// elements of the list.
			int[] input = new int[1];
			List<String> bindings = precomputed_tuples.getNameBindings();
			int i = 0;
			for (String name : bindings) {
				if (name.equals("listhead")) {
					input[0] = i;
					break;
				}
				++i;
			}
			TupleMap map = precomputed_tuples.getBindingsFromBindings(input,
					null);
			TupleSet precomputed_tuples2 = new TupleSet(
					precomputed_tuples.getNameBindings());

			int pos_head = 0;
			for (int j = 0; j < bindings.size(); ++j) {
				String name = bindings.get(j);
				if (name != null && name.equals("listhead")) {
					pos_head = j;
				}
			}

			MultiValue k1 = new MultiValue(new long[1]);
			for (Map.Entry<Long, List<Long>> entry : all_lists.entrySet()) {
				k1.values[0] = entry.getKey();
				CollectionTuples tuples = map.get(k1);
				if (tuples != null) {
					for (int j = tuples.getStart(); j < tuples.getEnd(); j += tuples
							.getSizeTuple()) {
						long[] original_tuple = new long[tuples.getSizeTuple()];
						System.arraycopy(tuples.getRawValues(), j,
								original_tuple, 0, tuples.getSizeTuple());
						for (long el : entry.getValue()) {
							original_tuple[pos_head] = el;
							precomputed_tuples2.addTuple(original_tuple);
						}
					}
				}
			}

			precomputed_tuples = precomputed_tuples2;

			if (genPatterns.size() > 0) {
				// Process the patterns that do not require the processing of a
				// list
				if (genPatterns.size() > 0) {
					List<Pattern[]> strategies = new ArrayList<Pattern[]>();
					strategies.add(genPatterns.toArray(new Pattern[genPatterns
							.size()]));
					GENERICS_STRATS = setup_generics(strategies);

					if (GENERICS_STRATS.length > 1) {
						log.warn("Use only one strategy to process the not-list patterns");
						GENERICS_STRATS = Arrays.copyOf(GENERICS_STRATS, 1);
					}
				}
			}

			return;
		}

		// Process the patterns that do not require the processing of a
		// list
		if (genPatterns.size() > 0) {
			List<Pattern[]> strategies = new ArrayList<Pattern[]>();
			strategies
					.add(genPatterns.toArray(new Pattern[genPatterns.size()]));
			GENERICS_STRATS = setup_generics(strategies);

			if (GENERICS_STRATS.length > 1) {
				log.warn("Use only one strategy to process the not-list patterns");
				GENERICS_STRATS = Arrays.copyOf(GENERICS_STRATS, 1);
			}
		}

		// Calculate the mapping between the other triples and the first pattern
		// of the list.
		if (GENERICS_STRATS != null) {
			last_generic_first_list_mapping = calculate_shared_vars_precomp_first_list(
					precomputed_tuples, GENERICS_STRATS[0].patterns,
					LIST_PATTERNS[0]);
		} else {
			last_generic_first_list_mapping = calculate_shared_vars_precomp_first_list(
					precomputed_tuples, null, LIST_PATTERNS[0]);

			// Calculate the mappings for the expansion of the rule.
			int[] input = new int[1];
			input[0] = precomputed_patterns_head[0].pos1;
			int[] output = new int[1];
			output[0] = pos_head_list_in_precomps;
			mapping_head_list_heads = precomputed_tuples
					.getBindingsFromBindings(input, output, true);
		}

		// In case the patterns are more than one, calculate the variables with
		// the next pattern
		if (LIST_PATTERNS.length > 1) {
			// Calculate the total list of bindings so far.
			List<String> existing_bindings = precomputed_tuples
					.getNameBindings();
			if (GENERICS_STRATS != null) {
				for (Pattern p : GENERICS_STRATS[0].patterns) {
					existing_bindings = TupleSet.concatenateBindings(
							existing_bindings, p);
				}
			}
			lshared_var_with_next_gen_pattern = new Mapping[LIST_PATTERNS.length - 1][];
			for (int i = 0; i < LIST_PATTERNS.length - 1; ++i) {
				existing_bindings = TupleSet.concatenateBindings(
						existing_bindings, LIST_PATTERNS[i]);
				Pattern p2 = LIST_PATTERNS[i + 1];
				List<Mapping> list = new ArrayList<Mapping>();
				for (int m = 0; m < existing_bindings.size(); ++m) {
					String t1 = existing_bindings.get(m);
					for (int n = 0; n < 3; ++n) {
						String t2 = p2.p[n].getName();
						if (t1 != null && t2 != null && t1.equals(t2)
								&& !t1.equals("el_n")) {
							Mapping map = new Mapping();
							map.nameBinding = t1;

							if (t1.indexOf('_') != -1) {
								map.pos1 = m - existing_bindings.size();
							} else {
								map.pos1 = m;
							}

							map.pos2 = n;
							list.add(map);
						}
					}
				}
				lshared_var_with_next_gen_pattern[i] = list
						.toArray(new Mapping[list.size()]);
			}
		}

		// Calculate the variables that should be mapped when I repeat the join
		// with a new element of the list.
		List<String> existing_bindings = precomputed_tuples.getNameBindings();
		if (GENERICS_STRATS != null) {
			for (Pattern p : GENERICS_STRATS[0].patterns) {
				existing_bindings = TupleSet.concatenateBindings(
						existing_bindings, p);
			}
		}
		existing_bindings = TupleSet.concatenateBindings(existing_bindings,
				LIST_PATTERNS[0]);
		Pattern p = LIST_PATTERNS[0];
		List<Mapping> list = new ArrayList<Mapping>();
		for (int i = 0; i < 3; ++i) {
			String t = p.p[i].getName();
			if (t != null) {
				for (int j = 0; j < existing_bindings.size(); ++j) {
					String name = existing_bindings.get(j);
					boolean equal = false;
					if (name.indexOf('_') != -1 && t.indexOf('_') != -1)
						equal = name.substring(0, name.indexOf('_')).equals(
								t.substring(0, t.indexOf('_')))
								&& name.endsWith("_n+1") && t.endsWith("_n");
					else
						equal = name.equals(t) && !name.equals("el_n");
					if (equal) {
						Mapping map = new Mapping();
						map.nameBinding = t;

						if (name.indexOf('_') != -1) {
							map.pos1 = j - existing_bindings.size();
						} else {
							map.pos1 = j;
						}

						map.pos2 = i;
						list.add(map);
					}
				}
			}
		}
		lshared_var_new_list = list.toArray(new Mapping[list.size()]);

		// Calculate the shared variables between the patterns in the list and
		// the HEAD and the unique variables
		lshared_var_head = new Mapping[LIST_PATTERNS.length][];
		lshared_var_firsthead = new Mapping[LIST_PATTERNS.length][];
		lshared_var_lasthead = new Mapping[LIST_PATTERNS.length][];

		pos_unique_vars = new int[LIST_PATTERNS.length][];

		Set<String> set = new HashSet<String>();
		set.addAll(precomputed_tuples.getNameBindings());
		if (GENERICS_STRATS != null) {
			for (Pattern pattern : GENERICS_STRATS[0].patterns) {
				set.addAll(pattern.getAllVars());
			}
		}

		for (int j = 0; j < LIST_PATTERNS.length; ++j) {
			Pattern pattern = LIST_PATTERNS[j];
			list = new ArrayList<Mapping>();
			List<Mapping> firstMapping = new ArrayList<Mapping>();
			List<Mapping> lastMapping = new ArrayList<Mapping>();

			List<Integer> pu = new ArrayList<Integer>();
			for (int i = 0; i < 3; ++i) {
				RDFTerm t_head = HEAD.p[i];
				for (int n = 0; n < 3; ++n) {
					RDFTerm t_p = pattern.p[n];
					if (t_head.getName() != null && t_p.getName() != null) {
						if (t_p.getName().equals(t_head.getName())
								&& t_head.getName().indexOf('_') == -1) {
							Mapping map = new Mapping();
							map.nameBinding = t_head.getName();
							map.pos1 = n;
							map.pos2 = i;
							list.add(map);
							firstMapping.add(map);
							lastMapping.add(map);
						} else {
							String h = t_head.getName();
							String t = t_p.getName();
							if (h.indexOf('_') != -1 && t.indexOf('_') != -1) {
								String ph = h.substring(1, h.indexOf('_'));
								String pt = t.substring(1, t.indexOf('_'));
								if (ph.equals(pt)) {
									String sh = h.substring(h.indexOf('_') + 1,
											h.length());
									String st = t.substring(t.indexOf('_') + 1,
											t.length());
									if (sh.equals("0") && st.equals("n")) {
										Mapping map = new Mapping();
										map.nameBinding = t_head.getName();
										map.pos1 = n;
										map.pos2 = i;
										firstMapping.add(map);
									} else if (sh.equals(st)
											&& sh.equals("n+1")) {
										Mapping map = new Mapping();
										map.nameBinding = t_head.getName();
										map.pos1 = n;
										map.pos2 = i;
										lastMapping.add(map);
									}
								}
							}
						}
					}

					if (t_p.getName() != null && !set.contains(t_p.getName())) {
						pu.add(n);
						set.add(t_p.getName());
					}
				}
			}

			lshared_var_head[j] = list.toArray(new Mapping[list.size()]);
			lshared_var_firsthead[j] = firstMapping
					.toArray(new Mapping[firstMapping.size()]);
			lshared_var_lasthead[j] = lastMapping
					.toArray(new Mapping[lastMapping.size()]);

			pos_unique_vars[j] = new int[pu.size()];
			for (int i = 0; i < pu.size(); ++i) {
				pos_unique_vars[j][i] = pu.get(i);
			}

		}

		// Filter out the values for the following times
		pos_unique_vars_next = new int[LIST_PATTERNS.length][];
		for (int i = 0; i < LIST_PATTERNS.length; ++i) {
			List<Integer> unique = new ArrayList<Integer>();
			for (int j = 0; j < pos_unique_vars[i].length; ++j) {
				String var = LIST_PATTERNS[i].p[pos_unique_vars[i][j]]
						.getName();
				if (var.endsWith("_n+1")) {
					unique.add(pos_unique_vars[i][j]);
				} else {
					if (var.endsWith("_n")) {
						// Check there is no _n+1
						String prefix = var.substring(0, var.indexOf('_'));
						boolean f = false;
						for (RDFTerm t : LIST_PATTERNS[i].p) {
							if (t.getName() != null
									&& t.getName().equals(prefix + "_n+1")) {
								f = true;
							}
						}

						if (!f)
							unique.add(pos_unique_vars[i][j]);
					}
				}
			}
			pos_unique_vars_next[i] = new int[unique.size()];
			for (int m = 0; m < unique.size(); ++m) {
				pos_unique_vars_next[i][m] = unique.get(m);
			}
		}
	}

	private Mapping[] calculate_shared_vars_precomp_first_list(
			TupleSet precomputed_tuples, Pattern[] additional_patterns,
			Pattern pattern) throws Exception {

		List<String> existing_bindings = precomputed_tuples.getNameBindings();
		if (additional_patterns != null) {
			for (Pattern p : additional_patterns) {
				existing_bindings = TupleSet.concatenateBindings(
						existing_bindings, p);
			}
		}

		List<Mapping> list = new ArrayList<Mapping>();
		for (int m = 0; m < 3; ++m) {
			String t1 = pattern.p[m].getName();
			for (int n = 0; n < existing_bindings.size(); ++n) {
				String t2 = existing_bindings.get(n);
				if (t1 != null && t2 != null) {
					if (t1.equals(t2)
							|| (t2.equals("listhead") && t1.equals("el_n"))) {
						Mapping map = new Mapping();
						map.nameBinding = t1;
						map.pos1 = n;
						map.pos2 = m;
						list.add(map);
					}
				}
			}
		}

		return list.toArray(new Mapping[list.size()]);
	}

	public void substituteListNameValueInPattern(Pattern origin,
			QueryNode destination, int pos, long value) {
		for (int i = 0; i < origin.p.length; ++i) {

			if (origin.p[i].getName() != null
					&& origin.p[i].getName().endsWith("_n")) {
				destination.setName(
						i,
						origin.p[i].getName().substring(0,
								origin.p[i].getName().indexOf('_'))
								+ "_" + pos);
			} else if (origin.p[i].getName() != null
					&& origin.p[i].getName().endsWith("_n+1")) {
				destination.setName(
						i,
						origin.p[i].getName().substring(0,
								origin.p[i].getName().indexOf('_'))
								+ "_" + (pos + 1));
			} else {
				destination.setName(i, origin.p[i].getName());
			}

			if (origin.p[i].getName() != null
					&& origin.p[i].getName().equals("el_n")) {
				destination.setTerm(i, value);
			} else {
				destination.setTerm(i, origin.p[i].getValue());
			}
		}
	}

	public void substituteListNameValueInPattern(Pattern origin,
			RDFTerm[] destination, int pos, long value) {
		for (int i = 0; i < origin.p.length; ++i) {

			if (origin.p[i].getName() != null
					&& origin.p[i].getName().endsWith("_n")) {
				destination[i].setName(origin.p[i].getName().substring(0,
						origin.p[i].getName().indexOf('_'))
						+ "_" + pos);
			} else if (origin.p[i].getName() != null
					&& origin.p[i].getName().endsWith("_n+1")) {
				destination[i].setName(origin.p[i].getName().substring(0,
						origin.p[i].getName().indexOf('_'))
						+ "_" + (pos + 1));
			} else {
				destination[i].setName(origin.p[i].getName());
			}

			if (origin.p[i].getName() != null
					&& origin.p[i].getName().equals("el_n")) {
				destination[i].setValue(value);
			} else {
				destination[i].setValue(origin.p[i].getValue());
			}
		}
	}
}