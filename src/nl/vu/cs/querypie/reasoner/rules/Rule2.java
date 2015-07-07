package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.TupleMap;
import nl.vu.cs.querypie.storage.memory.TupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule2 extends Rule1 {

    static final Logger log = LoggerFactory.getLogger(Rule2.class);

    public GenericVars[] GENERICS_STRATS;

    static public class GenericVars {
	public Pattern[] patterns;
	public Mapping[][] pos_shared_vars_precomp_generics;
	public TupleMap mapping_head_first_generic;
	public TupleMap mapping_head1_first_generic;
	public TupleMap mapping_head2_first_generic;
	public TupleMap mapping_head3_first_generic;
	public long id_all_values_first_generic_pattern;
	public long id_all_values_first_generic_pattern2;

	public int[][] pos_variables_generic_patterns;
	public Mapping[] shared_var_with_next_gen_pattern;
	public Mapping[][] pos_shared_vars_generics_head;
	public int[][] pos_unique_vars_generic_patterns;
	public boolean filter_values_last_generic_pattern;

	public boolean are_generics_equivalent;

	// Used for the recursion
	public boolean head_used_generic_pattern;
	public int[][] are_generic_patterns_recursive;
    }

    public Rule2(int id, String head, String[] precomps, String[] locations)
	    throws Exception {
	super(id, head, precomps, locations);
	type = 2;
    }

    public Rule2(int id, String head, String[] precomps, String[] locations,
	    String[] generics) throws Exception {

	super(id, head, precomps, locations);
	type = 2;

	if (!isActive) {
	    return;
	}

	Pattern[] patterns = parse_patterns(generics, null);
	List<Pattern[]> allStrategies = calculate_exec_strategies(patterns);
	GENERICS_STRATS = setup_generics(allStrategies);
    }

    private int[][] calculate_positions_vars(Pattern[] patterns) {
	int[][] result = new int[patterns.length][];
	for (int i = 0; i < patterns.length; ++i) {
	    result[i] = patterns[i].getPositionVars();
	}
	return result;
    }

    private Mapping[][] calculate_sharing_vars_with_existing_patterns(
	    TupleSet precomputed, Pattern[] existing, Pattern[] patterns) {
	Mapping[][] result = new Mapping[patterns.length][];

	TupleSet set = precomputed;

	if (existing != null) {
	    for (Pattern p : existing) {
		set = new TupleSet(TupleSet.concatenateBindings(
			set.getNameBindings(), p));
	    }
	}

	for (int i = 0; i < patterns.length; ++i) {
	    result[i] = set.calculateSharedVariables(patterns[i]);
	    set = new TupleSet(TupleSet.concatenateBindings(
		    set.getNameBindings(), patterns[i]));
	}

	return result;
    }

    private Mapping[][] calculate_sharing_vars_with_pattern(Pattern head,
	    Pattern[] patterns, int[][] positions_vars,
	    Mapping[] existingMappings) {
	Mapping[][] result = new Mapping[patterns.length][];
	int[] hvars = head.getPositionVars();

	Set<Integer> alreadyGiven = new HashSet<Integer>();
	for (int i = 0; i < result.length; ++i) {
	    int[] vars = positions_vars[i];
	    List<Mapping> pos = new ArrayList<Mapping>();
	    for (int gvar : vars) {
		for (int var : hvars) {
		    if (patterns[i].p[gvar].getName().equals(
			    head.p[var].getName())
			    && !alreadyGiven.contains(var)) {
			boolean ok = true;
			for (int j = 0; j < existingMappings.length && ok; ++j) {
			    if (var == existingMappings[j].pos2)
				ok = false;
			}
			if (ok) {
			    Mapping map = new Mapping();
			    map.pos1 = gvar;
			    map.pos2 = var;
			    map.nameBinding = head.p[var].getName();
			    pos.add(map);
			    alreadyGiven.add(var);
			    break;
			}
		    }
		}
	    }
	    if (pos.size() > 0) {
		result[i] = pos.toArray(new Mapping[pos.size()]);
	    }
	}

	return result;
    }

    private long calculate_id_all_elements_with_precomps(
	    TupleSet precomputed_tuples,
	    Mapping[] pos_shared_vars_precomp_generics, int pos,
	    Pattern[] patterns) throws Exception {

	long result = 0;

	if (pos_shared_vars_precomp_generics.length > 2)
	    throw new Exception("Not supported");

	result = Schema.getInstance().calculateIdToSetAllElements(
		precomputed_tuples, patterns,
		pos_shared_vars_precomp_generics[pos].pos1);

	return result;
    }

    private void calculate_mappings_head(GenericVars g,
	    TupleSet precomputed_tuples, Mapping[] precomputed_patterns_head,
	    Mapping[][] pos_shared_vars_precomp_generics) {
	if (precomputed_patterns_head != null
		&& pos_shared_vars_precomp_generics[0].length > 0) {
	    int[] names = new int[pos_shared_vars_precomp_generics[0].length];
	    for (int i = 0; i < names.length; ++i)
		names[i] = pos_shared_vars_precomp_generics[0][i].pos1;
	    int[] input = new int[precomputed_patterns_head.length];
	    for (int i = 0; i < input.length; ++i)
		input[i] = precomputed_patterns_head[i].pos1;

	    g.mapping_head_first_generic = precomputed_tuples
		    .getBindingsFromBindings(input, names, true);

	    if (input.length > 1) {
		// Need to index singularly by every field
		input = new int[1];
		input[0] = precomputed_patterns_head[0].pos1;
		g.mapping_head1_first_generic = precomputed_tuples
			.getBindingsFromBindings(input, names, true);
		input[0] = precomputed_patterns_head[1].pos1;
		g.mapping_head2_first_generic = precomputed_tuples
			.getBindingsFromBindings(input, names, true);
		input = new int[2];
		input[0] = precomputed_patterns_head[0].pos1;
		input[1] = precomputed_patterns_head[1].pos1;
		g.mapping_head3_first_generic = precomputed_tuples
			.getBindingsFromBindings(input, names, true);
	    }
	}
    }

    private int[][] calculate_unique_vars(
	    int[][] pos_variables_generic_patterns,
	    Mapping[][] pos_shared_vars_precomp_generics) {
	int[][] result = new int[pos_variables_generic_patterns.length][];
	for (int j = 0; j < pos_variables_generic_patterns.length; ++j) {
	    int[] pos_vars = pos_variables_generic_patterns[j];
	    Mapping[] pos_shared_precomps = pos_shared_vars_precomp_generics[j];
	    // Mapping[] pos_shared_head =
	    // GENERICS_VARS.pos_shared_vars_generics_head[j];
	    List<Integer> pos = new ArrayList<Integer>();
	    if (pos_vars != null && pos_vars.length > 0) {
		for (int pos_var : pos_vars) {
		    boolean found = false;
		    for (Mapping map : pos_shared_precomps) {
			if (map.pos2 == pos_var) {
			    found = true;
			    break;
			}
		    }
		    if (!found) {
			pos.add(pos_var);
		    }
		}
	    }
	    result[j] = new int[pos.size()];
	    for (int i = 0; i < pos.size(); ++i) {
		result[j][i] = pos.get(i);
	    }
	}

	return result;
    }

    private Mapping[] calculate_sharing_vars_with_next_pattern(
	    TupleSet precomputed_tuples, Pattern[] patterns) {
	Mapping[] result = new Mapping[patterns.length - 1];
	for (int i = 0; i < patterns.length - 1; ++i) {
	    Pattern old_p = patterns[i];
	    Pattern p = patterns[i + 1];

	    List<Mapping> mappings = new ArrayList<Mapping>();
	    for (int m = 0; m < 3; ++m) {
		String old_name = old_p.p[m].getName();
		if (old_name != null) {
		    boolean found = false;
		    for (int n = 0; n < 3 && !found; n++) {
			String name = p.p[n].getName();
			if (name != null && old_name.equals(name)) {
			    found = true;
			    Mapping map = new Mapping();
			    map.nameBinding = name;
			    map.pos1 = m;
			    map.pos2 = n;
			    mappings.add(map);
			}
		    }
		}
	    }

	    if (mappings.size() == 1) {
		result[i] = mappings.get(0);
	    } else {
		List<String> vars = precomputed_tuples.getNameBindings();
		for (Mapping map : mappings) {
		    boolean found = false;
		    for (int m = 0; m < vars.size() && !found; ++m) {
			if (vars.get(m).equals(map.nameBinding)) {
			    found = true;
			}
		    }
		    if (!found) {
			result[i] = map;
			break;
		    }
		}
	    }
	}
	return result;
    }

    protected GenericVars[] setup_generics(List<Pattern[]> allStrategies)
	    throws Exception {

	GenericVars[] GENERICS_STRATS = new GenericVars[allStrategies.size()];
	int i = 0;
	for (Pattern[] strategy : allStrategies) {
	    GENERICS_STRATS[i++] = setup_generics(null, strategy);
	}

	return GENERICS_STRATS;
    }

    protected GenericVars setup_generics(Pattern[] existingPatterns, Pattern[] g)
	    throws Exception {
	GenericVars generic_details = new GenericVars();
	generic_details.patterns = g;

	// Calculate the position of the variables in the generic patterns
	generic_details.pos_variables_generic_patterns = calculate_positions_vars(g);

	// Calculate the mappings between the generics pattern and the
	// precomputed (or actual) triples
	generic_details.pos_shared_vars_precomp_generics = calculate_sharing_vars_with_existing_patterns(
		precomputed_tuples, existingPatterns, g);

	if (existingPatterns == null) {
	    // Index the precomputed set with the variable from the head to the
	    // variable to the first generic triple
	    calculate_mappings_head(generic_details, precomputed_tuples,
		    precomputed_patterns_head,
		    generic_details.pos_shared_vars_precomp_generics);

	    // ID of all the values between the precomputed and first generic
	    // pattern
	    generic_details.id_all_values_first_generic_pattern = calculate_id_all_elements_with_precomps(
		    precomputed_tuples,
		    generic_details.pos_shared_vars_precomp_generics[0], 0,
		    PRECOMPS);
	    if (generic_details.pos_shared_vars_precomp_generics[0].length == 2) {
		generic_details.id_all_values_first_generic_pattern2 = calculate_id_all_elements_with_precomps(
			precomputed_tuples,
			generic_details.pos_shared_vars_precomp_generics[0], 1,
			PRECOMPS);
	    }
	}

	// Check whether the generic patterns share some variables directly with
	// the head of the rule
	generic_details.pos_shared_vars_generics_head = calculate_sharing_vars_with_pattern(
		HEAD, g, generic_details.pos_variables_generic_patterns,
		precomputed_patterns_head);

	// Calculate how many unique variables each generic pattern has.
	generic_details.pos_unique_vars_generic_patterns = calculate_unique_vars(
		generic_details.pos_variables_generic_patterns,
		generic_details.pos_shared_vars_precomp_generics);

	// For each generic pattern calculate the shared variable with the
	// next one. The variables shared also with the precomp. are not
	// considered.
	generic_details.shared_var_with_next_gen_pattern = calculate_sharing_vars_with_next_pattern(
		precomputed_tuples, g);

	// Determine whether I can filter out values when I calculate the
	// last generic pattern
	if (precomputed_patterns_head == null
		|| precomputed_patterns_head.length == 1) {
	    generic_details.filter_values_last_generic_pattern = true;
	    for (int i = 0; i < 3; ++i) {
		RDFTerm t_h = HEAD.p[i];
		RDFTerm t_g = g[g.length - 1].p[i];
		if (t_h.getName() != null) {
		    if (t_g.getName() == null
			    || (!t_h.getName().equals(t_g.getName()) && (precomputed_patterns_head == null || precomputed_patterns_head[0].pos2 != i))) {
			generic_details.filter_values_last_generic_pattern = false;
			break;
		    }
		} else if (t_g.getName() != null
			|| t_g.getValue() != t_h.getValue()) {
		    generic_details.filter_values_last_generic_pattern = false;
		    break;
		}
	    }
	} else {
	    generic_details.filter_values_last_generic_pattern = false;
	}

	// Check with the first and second generic patterns are equivalent. In
	// this case we avoid one call.
	generic_details.are_generics_equivalent = generic_details.patterns.length == 2;
	if (generic_details.are_generics_equivalent) {

	    for (int i = 0; i < 3; ++i) {
		RDFTerm t1 = generic_details.patterns[0].p[i];
		RDFTerm t2 = generic_details.patterns[1].p[i];

		if (t1.getValue() != t2.getValue()) {
		    generic_details.are_generics_equivalent = false;
		    break;
		}
	    }
	    if (generic_details.pos_shared_vars_precomp_generics[0] != null) {
		for (Mapping map : generic_details.pos_shared_vars_precomp_generics[0]) {
		    if (!generic_details.patterns[0].p[map.pos2].getName()
			    .equals(generic_details.patterns[1].p[map.pos2]
				    .getName())) {
			generic_details.are_generics_equivalent = false;
			break;
		    }
		}
	    }
	}

	// Check whether the HEAD is equivalent to one of the generic pattern.
	// If it is true then rule is recursive.
	generic_details.head_used_generic_pattern = false;
	generic_details.are_generic_patterns_recursive = new int[generic_details.patterns.length][3];
	for (int j = 0; j < generic_details.patterns.length; ++j) {
	    Pattern genericPattern = generic_details.patterns[j];

	    boolean patternOk = true;
	    for (int i = 0; i < 3 && patternOk; ++i) {
		RDFTerm t1 = genericPattern.p[i];
		RDFTerm th = HEAD.p[i];

		if (th.getValue() != t1.getValue()) {
		    if (th.getValue() == Schema.ALL_RESOURCES
			    && t1.getValue() >= 0) {
			for (Mapping map : precomputed_patterns_head) {
			    if (map.pos2 == i) {
				// Check whether such value is compatible with
				// the values in the precomp patterns
				Collection<Long> col = precomputed_tuples
					.getAllValues(map.pos1, true);
				if (col == null || !col.contains(t1.getValue())) {
				    patternOk = false;
				    break;
				}
			    }
			}
		    } else if (th.getValue() >= 0
			    && t1.getValue() == Schema.ALL_RESOURCES) {
			List<String> names = precomputed_tuples
				.getNameBindings();
			for (int m = 0; m < names.size(); ++m) {
			    String name = names.get(m);
			    if (name.equals(t1.getName())) {
				Collection<Long> values = precomputed_tuples
					.getAllValues(m, true);
				if (values == null
					|| !values.contains(th.getValue())) {
				    patternOk = false;
				    break;
				}
			    }
			}
		    } else {
			patternOk = false;
			break;
		    }
		}

		// Check whether the term in t1 is shared with a precomputed
		// pattern and record the position
		boolean matchingFound = false;
		if (t1.getValue() == Schema.ALL_RESOURCES
			&& th.getValue() == Schema.ALL_RESOURCES
			&& !th.getName().equals(t1.getName())) {
		    List<String> names = precomputed_tuples.getNameBindings();
		    for (int m = 0; m < names.size() && !matchingFound; ++m) {
			String name = names.get(m);
			if (name.equals(t1.getName())) {
			    matchingFound = true;
			    generic_details.are_generic_patterns_recursive[j][i] = m;
			}
		    }
		}
		if (!matchingFound)
		    generic_details.are_generic_patterns_recursive[j][i] = -1;
	    }

	    if (patternOk) {
		generic_details.head_used_generic_pattern = true;
	    } else {
		generic_details.are_generic_patterns_recursive[j] = null;
	    }
	}

	return generic_details;
    }

    private List<Pattern[]> get_remainer_list(int position_head,
	    Pattern[] input_list) {

	List<Pattern[]> l = new ArrayList<Pattern[]>();
	if (input_list.length == 1) {
	    l.add(input_list);
	    return l;
	}

	Pattern head = input_list[position_head];
	List<Pattern> r = new ArrayList<Pattern>();
	for (int j = 0; j < input_list.length; ++j) {
	    if (j != position_head) {
		r.add(input_list[j]);
	    }
	}
	Pattern[] remainings = r.toArray(new Pattern[r.size()]);

	for (int i = 0; i < remainings.length; ++i) {
	    Pattern p = remainings[i];
	    if (head.calculateSharedVars(p).size() > 0) {
		List<Pattern[]> possibilities = get_remainer_list(i, remainings);
		for (Pattern[] possibility : possibilities) {
		    Pattern[] list = new Pattern[input_list.length];
		    list[0] = head;
		    System.arraycopy(possibility, 0, list, 1,
			    input_list.length - 1);
		    l.add(list);
		}
	    }
	}

	return l;
    }

    protected List<Pattern[]> calculate_exec_strategies(Pattern[] gs) {
	List<Pattern[]> lists = new ArrayList<Pattern[]>();

	for (int i = 0; i < gs.length; ++i) {
	    Pattern g = gs[i];
	    if (precomputed_tuples.calculateSharedVariables(g).length > 0) {
		List<Pattern[]> possibilities = get_remainer_list(i, gs);
		for (Pattern[] possibility : possibilities) {
		    lists.add(possibility);
		}
	    }
	}

	if (lists.size() == 0) {
	    lists.add(gs);
	}

	return lists;
    }

    public String toString() {
	return HEAD
		+ ":-"
		+ Arrays.toString(PRECOMPS)
		+ ","
		+ (GENERICS_STRATS != null ? Arrays
			.toString(GENERICS_STRATS[0].patterns) : "null");
    }
}