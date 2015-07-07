package nl.vu.cs.querypie.reasoner.rules;

import java.util.Arrays;
import java.util.List;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.Mapping;
import nl.vu.cs.querypie.storage.memory.SortedCollectionTuples;
import nl.vu.cs.querypie.storage.memory.TupleSet;
import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Rule1 extends Rule {

    static final Logger log = LoggerFactory.getLogger(Rule1.class);

    public Pattern[] PRECOMPS;
    public TupleSet precomputed_tuples;
    public SortedCollectionTuples explicitBindings;
    public Mapping[] precomputed_patterns_head;
    public long[] head_precomputed_id_sets = new long[3];
    public boolean isHeadPrecomputed;

    public Rule1(int id, String head, String[] precomps, String[] locations)
	    throws Exception {
	super(id, head);
	type = 1;

	Arrays.fill(head_precomputed_id_sets, -1);

	// Parse the precomp patterns
	PRECOMPS = parse_patterns(precomps, locations);

	// Calculate whether there is one precomputed pattern with the same
	// structure than the HEAD. We can use it to remove the duplicates,
	// when we load the data.
	List<String[]> mapping = null;
	int pos_pattern = 0;
	for (Pattern p : PRECOMPS) {
	    mapping = HEAD.calculateEquivalenceMapping(p);
	    if (mapping != null)
		break;
	    pos_pattern++;
	}
	if (mapping != null) {
	    String[] orign = new String[mapping.size()];
	    String[] dest = new String[mapping.size()];
	    for (int i = 0; i < mapping.size(); ++i) {
		dest[i] = mapping.get(i)[0];
		orign[i] = mapping.get(i)[1];
	    }
	    precomputed_tuples = Schema.getInstance().joinPrecomputedPatterns(
		    PRECOMPS, dest, pos_pattern);
	} else {
	    precomputed_tuples = Schema.getInstance().joinPrecomputedPatterns(
		    PRECOMPS, null, pos_pattern);
	}

	// If there are no precomputed tuples the rule is deactivated
	if (precomputed_tuples == null || precomputed_tuples.size() == 0) {
	    isActive = false;
	    return;
	} else {
	    isActive = true;
	}

	// Is the HEAD a precomputed pattern? Then we can filter out some
	// duplicates when we execute the rule
	if (Schema.getInstance().isPrecomputedPattern(HEAD)) {
	    explicitBindings = Schema.getInstance().getVarsPrecomputedPattern(
		    HEAD);
	    if (explicitBindings != null) {
		isHeadPrecomputed = true;
	    }
	}

	// Calculate the mappings between the precomputed triples and the
	// head of the rule
	precomputed_patterns_head = precomputed_tuples
		.calculateSharedVariables(HEAD);

	// I calculate them so that when expand a chain I can calculate the
	// intersection between rules.
	for (Mapping map : precomputed_patterns_head) {
	    head_precomputed_id_sets[map.pos2] = Schema.getInstance()
		    .calculateIdToSetAllElements(precomputed_tuples, PRECOMPS,
			    map.pos1);

	}
    }

    protected Pattern[] parse_patterns(String[] patterns, String[] locations) {
	Pattern[] default_gen = new Pattern[patterns.length];
	for (int i = 0; i < patterns.length; ++i) {
	    default_gen[i] = Utils.parsePattern(patterns[i]);
	    if (locations != null) {
		// Add elements about the locations
		String loc = locations[i];
		boolean equivalence = false;
		boolean filter = false;
		if (loc.endsWith("*")) {
		    loc = loc.substring(0, loc.length() - 1);
		    filter = true;
		}

		if (loc.endsWith("+")) {
		    loc = loc.substring(0, loc.length() - 1);
		    equivalence = true;
		}

		default_gen[i].setFilter(filter);
		default_gen[i].setLocation(loc);
		default_gen[i].setEquivalent(equivalence);
	    }

	}
	return default_gen;
    }
}