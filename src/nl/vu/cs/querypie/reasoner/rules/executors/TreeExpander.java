package nl.vu.cs.querypie.reasoner.rules.executors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import nl.vu.cs.querypie.QueryPIE;
import nl.vu.cs.querypie.reasoner.AddIntermediateTriples;
import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.actions.SendTo;
import arch.chains.Chain;
import arch.data.types.TBoolean;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class TreeExpander extends Action {

    static final Logger log = LoggerFactory.getLogger(TreeExpander.class);
    public static final int CONCURRENT_ACTIONS_BFS = 8;

    public static class SequencePatterns {
	public RDFTerm[] tripleOutput = { new RDFTerm(), new RDFTerm(),
		new RDFTerm() };
	public SequencePatterns next = null;
    }

    public static class RuleChildren {
	public Rule rule = null;
	public RDFTerm[] head;
	public int ref_to_memory = -1;
	public long list_head = -1;
	public int list_id = -1;
	public int current_pattern = 0;
	public boolean single_node = false;
	public int strag_id = -1;
	public SequencePatterns patterns = null;
    }

    // private static final Logger log = LoggerFactory
    // .getLogger(TreeExpander.class);
    private static Schema schema = Schema.getInstance();
    private static Ruleset ruleset = Ruleset.getInstance();

    RDFTerm[] triple = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
    RDFTerm[] tripleOutput = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
    MultiValue k1 = new MultiValue(new long[1]);
    MultiValue k2 = new MultiValue(new long[2]);

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {

	// Check it is the correct layer
	if (inputChain.getInputLayerId() != Consts.DEFAULT_INPUT_LAYER_ID) {
	    return inputChain;
	}

	// // Incremental reasoning
	// boolean incrementalReasoning = false;
	// if (tuple.getNElements() > 3) {
	// incrementalReasoning = true;
	// }

	if (!QueryPIE.ENABLE_COMPLETENESS
		&& inputChain.countNumberActions("Rule") >= QueryPIE.MAXIMUM_HEIGHT) {
	    return inputChain;
	}

	tuple.get(triple);

	// Check whether the subject has a special flag
	if (triple[0].getValue() == Schema.SCHEMA_SUBSET) {
	    return inputChain;
	}

	if (inputChain.detectLoop("Rule", tuple, 3, context)) {
	    return inputChain;
	}
	
	Chain supportChain = new Chain();
	Tuple supportTuple = new Tuple();

	// WritableContainer<Chain> children = null;
	//
	// if (incrementalReasoning) {
	// children = chainsToSend; // Execute the chains immediately
	// } else {
	// children = chainsToResolve;
	// }

	for (Rule1 rule : ruleset.getAllActiveFirstTypeRules()) {
	    if (!checkHead(rule, triple, context)) {
		continue;
	    }
	    RuleChildren c = applyRuleFirstType(rule, triple);
	    if (c != null) {
		generate_new_chains(inputChain, c, chainsToResolve, context,
			supportTuple, supportChain);
	    }

	}

	for (Rule2 rule : ruleset.getAllActiveSecondTypeRules()) {
	    if (!checkHead(rule, triple, context)) {
		continue;
	    }
	    RuleChildren c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
		    triple, context, tripleOutput, k1, k2);
	    if (c != null) {
		generate_new_chains(inputChain, c, chainsToResolve, context,
			supportTuple, supportChain);
	    }
	}

	for (Rule3 rule : ruleset.getAllActiveThirdTypeRules()) {
	    if (!checkHead(rule, triple, context)) {
		continue;
	    }
	    RuleChildren c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS,
		    triple, context, tripleOutput, k1, k2);
	    if (c != null) {
		generate_new_chains(inputChain, c, chainsToResolve, context,
			supportTuple, supportChain);
	    }
	}

	for (Rule4 rule : ruleset.getAllActiveFourthTypeRules()) {
	    if (!checkHead(rule, triple, context)) {
		continue;
	    }
	    applyRuleFourthType(rule, triple, context, tripleOutput,
		    supportTuple, k1, k2, inputChain, chainsToResolve,
		    supportChain);
	}

	return inputChain;
    }

    public static RuleChildren applyRuleFirstType(Rule1 rule, RDFTerm[] head)
	    throws Exception {
	RuleChildren child = new RuleChildren();
	child.patterns = new SequencePatterns();
	child.patterns.tripleOutput[0].setValue(Schema.SCHEMA_SUBSET);
	child.rule = rule;
	child.head = head;
	return child;
    }

    public static boolean checkHead(Rule1 rule, RDFTerm[] triple,
	    ActionContext context) {
	boolean ok = true;
	for (int i = 0; i < 3; ++i) {
	    RDFTerm t = triple[i];
	    RDFTerm head_value = rule.HEAD.p[i];
	    if (t.isSet()) {
		if (head_value.getValue() >= 0) {
		    if (!schema.chekValueInSet(t, head_value.getValue(),
			    context)) {
			ok = false;
			break;
		    }
		} else if (rule.head_precomputed_id_sets[i] <= Schema.SET_THRESHOLD) {
		    if (!schema.isIntersection(t.getValue(),
			    rule.head_precomputed_id_sets[i], context)) {
			ok = false;
			break;
		    }
		}
	    } else if (t.getValue() >= 0) {
		if (head_value.getValue() != t.getValue()
			&& head_value.getValue() != Schema.ALL_RESOURCES) {
		    ok = false;
		    break;
		}

		if (rule.head_excludedValues != null
			&& rule.head_excludedValues[i].contains(t.getValue())) {
		    ok = false;
		    break;
		}
	    }
	}
	return ok;
    }

    private static int calculate_best_strategy(GenericVars[] generics,
	    RDFTerm[] triple) {

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
		    if (triple[map.pos2].getValue() != Schema.ALL_RESOURCES) {
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

    private static void unify(Pattern pattern, RDFTerm[] triple) {
	// Unify tripleOutput with the values of the GENERIC PATTERN
	for (int i = 0; i < 3; ++i) {
	    long v = pattern.p[i].getValue();
	    triple[i].setValue(v);
	}
    }

    public static void unify(RDFTerm[] input, RDFTerm[] output,
	    Mapping[] positions) {
	if (positions != null) {
	    for (Mapping pos : positions) {
		output[pos.pos1].setValue(input[pos.pos2].getValue());
	    }
	}
    }

    private static void applyRuleFourthType(Rule4 rule, RDFTerm[] head,
	    ActionContext context, RDFTerm[] supportTriple, Tuple supportTuple,
	    MultiValue k1, MultiValue k2, Chain inputChain,
	    WritableContainer<Chain> chainsToResolve, Chain supportChain)
	    throws Exception {
	RuleChildren c = null;
	if (rule.GENERICS_STRATS == null && rule.LIST_PATTERNS == null) {
	    c = applyRuleFirstType(rule, head);
	    if (c != null) {
		generate_new_chains(inputChain, c, chainsToResolve, context,
			supportTuple, supportChain);
	    }
	} else if (rule.GENERICS_STRATS != null) {
	    c = applyRuleWithGenerics(rule, rule.GENERICS_STRATS, head,
		    context, supportTriple, k1, k2);
	    if (c != null) {
		generate_new_chains(inputChain, c, chainsToResolve, context,
			supportTuple, supportChain);
	    }
	} else {
	    applyRuleWithGenericsList(rule, head, supportTriple, supportTuple,
		    context, k1, inputChain, chainsToResolve, supportChain);
	}
    }

    private static void applyRuleWithGenericsList(Rule4 rule, RDFTerm[] head,
	    RDFTerm[] supportTriple, Tuple supportTuple, ActionContext context,
	    MultiValue k1, Chain inputChain,
	    WritableContainer<Chain> outputChains, Chain supportChain)
	    throws Exception {

	if (rule.precomputed_patterns_head != null
		&& rule.precomputed_patterns_head.length == 2) {
	    throw new Exception("Not supported");
	}

	// Unify the pattern with the variables that come from the precomputed
	// patterns
	boolean firstOption = rule.precomputed_patterns_head.length == 0
		|| (rule.precomputed_patterns_head.length == 1 && head[rule.precomputed_patterns_head[0].pos2]
			.getValue() == Schema.ALL_RESOURCES);

	// Get the possible lists that is possible to follow.
	Collection<Long> list_heads = null;
	if (firstOption) {
	    list_heads = rule.all_lists.keySet();
	    for (long list_head : list_heads) {
		List<Long> list = rule.all_lists.get(list_head);
		if (list != null) {
		    rule.substituteListNameValueInPattern(
			    rule.LIST_PATTERNS[0], supportTriple, 0,
			    list.get(0));
		    unify(head, supportTriple, rule.lshared_var_firsthead[0]);
		    supportTuple.set(supportTriple);
		    generate_new_chain(rule, 0, rule.type != 2, supportTuple,
			    supportTriple, 0, head, -1, inputChain,
			    supportChain, outputChains, context, list_head, 0);
		}
	    }
	} else {
	    k1.values[0] = head[rule.precomputed_patterns_head[0].pos2]
		    .getValue();
	    CollectionTuples col = rule.mapping_head_list_heads.get(k1);

	    if (col != null) {
		for (int i = col.getStart(); i < col.getEnd(); ++i) {
		    List<Long> list = rule.all_lists.get(col.getRawValues()[i]);
		    if (list != null) {
			rule.substituteListNameValueInPattern(
				rule.LIST_PATTERNS[0], supportTriple, 0,
				list.get(0));
			unify(head, supportTriple,
				rule.lshared_var_firsthead[0]);
			supportTuple.set(supportTriple);
			generate_new_chain(rule, 0, rule.type != 2,
				supportTuple, supportTriple, 0, head, -1,
				inputChain, supportChain, outputChains,
				context, col.getRawValues()[i], 0);
		    }
		}
	    }
	}

    }

    public static RuleChildren applyRuleWithGenerics(Rule1 rule,
	    GenericVars[] generics, RDFTerm[] head, ActionContext context,
	    RDFTerm[] supportTriple, MultiValue k1, MultiValue k2)
	    throws Exception {
	// First determine what is the best strategy to execute the patterns
	// given a instantiated HEAD
	int strategy = calculate_best_strategy(generics, head);
	GenericVars g = generics[strategy];
	return applyRuleWithGenerics(rule, strategy, g, g.patterns[0], -1, -1,
		head, context, supportTriple, k1, k2);

    }

    private static RuleChildren applyRuleWithGenerics(Rule1 rule, int strategy,
	    GenericVars g, Pattern p, long list_head, int list_id,
	    RDFTerm[] instantiated_head, ActionContext context,
	    RDFTerm[] supportTriple, MultiValue k1, MultiValue k2)
	    throws Exception {

	RuleChildren output = new RuleChildren();
	output.rule = rule;
	output.head = instantiated_head;
	output.strag_id = strategy;
	output.list_head = list_head;
	output.list_id = list_id;
	output.single_node = rule.type != 2;

	// Unify the pattern with the variables that come from the precomputed
	// patterns
	boolean firstOption = rule.precomputed_tuples == null
		|| rule.precomputed_patterns_head.length == 0
		|| (rule.precomputed_patterns_head.length == 1 && instantiated_head[rule.precomputed_patterns_head[0].pos2]
			.getValue() == Schema.ALL_RESOURCES)
		|| (rule.precomputed_patterns_head.length == 2
			&& instantiated_head[rule.precomputed_patterns_head[0].pos2]
				.getValue() == Schema.ALL_RESOURCES && instantiated_head[rule.precomputed_patterns_head[1].pos2]
			.getValue() == Schema.ALL_RESOURCES);

	if (firstOption) {
	    output.patterns = new SequencePatterns();
	    RDFTerm[] tripleOutput = output.patterns.tripleOutput;
	    // Unify tripleOutput with the values of the GENERIC PATTERN
	    unify(p, tripleOutput);

	    // Unify the triple with the values from the head that are not used
	    // anywhere else
	    unify(instantiated_head, tripleOutput,
		    g.pos_shared_vars_generics_head[0]);

	    if (g.pos_shared_vars_precomp_generics[0].length > 0) {
		tripleOutput[g.pos_shared_vars_precomp_generics[0][0].pos2]
			.setValue(g.id_all_values_first_generic_pattern);
	    }

	    if (g.pos_shared_vars_precomp_generics[0].length > 1) {
		tripleOutput[g.pos_shared_vars_precomp_generics[0][1].pos2]
			.setValue(g.id_all_values_first_generic_pattern2);
	    }

	    return output;
	} else {

	    // Unify tripleOutput with the values of the GENERIC PATTERN
	    unify(p, supportTriple);

	    // Unify the triple with the values from the head that are not used
	    // anywhere else
	    unify(instantiated_head, supportTriple,
		    g.pos_shared_vars_generics_head[0]);

	    if (rule.precomputed_patterns_head.length == 1) {
		long v = instantiated_head[rule.precomputed_patterns_head[0].pos2]
			.getValue();
		Collection<Long> possibleKeys = null;
		if (v >= 0) {
		    possibleKeys = new ArrayList<Long>();
		    possibleKeys.add(v);
		} else {
		    possibleKeys = schema.getSubset(v, context);
		}

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

			    for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
				supportTriple[g.pos_shared_vars_precomp_generics[0][i].pos2]
					.setValue(possibleBindings.getValue(y,
						i));
			    }

			    // Copy supportTriple in patterns
			    SequencePatterns pattern = new SequencePatterns();
			    pattern.tripleOutput[0].setValue(supportTriple[0]
				    .getValue());
			    pattern.tripleOutput[1].setValue(supportTriple[1]
				    .getValue());
			    pattern.tripleOutput[2].setValue(supportTriple[2]
				    .getValue());
			    pattern.next = output.patterns;
			    output.patterns = pattern;
			}
		    }
		}
		return output.patterns == null ? null : output;
	    } else if (rule.precomputed_patterns_head.length == 2) {

		if (instantiated_head[rule.precomputed_patterns_head[0].pos2]
			.getValue() >= 0
			&& instantiated_head[rule.precomputed_patterns_head[1].pos2]
				.getValue() >= 0) {
		    // Use both values to restrict the search
		    k2.values[0] = instantiated_head[rule.precomputed_patterns_head[0].pos2]
			    .getValue();
		    k2.values[1] = instantiated_head[rule.precomputed_patterns_head[1].pos2]
			    .getValue();
		    CollectionTuples possibleBindings = g.mapping_head_first_generic
			    .get(k2);
		    if (possibleBindings != null) {
			for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
			    for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
				supportTriple[g.pos_shared_vars_precomp_generics[0][i].pos2]
					.setValue(possibleBindings.getValue(y,
						i));
			    }

			    // Copy supportTriple in patterns
			    SequencePatterns pattern = new SequencePatterns();
			    pattern.tripleOutput[0].setValue(supportTriple[0]
				    .getValue());
			    pattern.tripleOutput[1].setValue(supportTriple[1]
				    .getValue());
			    pattern.tripleOutput[2].setValue(supportTriple[2]
				    .getValue());
			    pattern.next = output.patterns;
			    output.patterns = pattern;
			}
			return output.patterns == null ? null : output;
		    }
		} else {
		    CollectionTuples possibleBindings = null;

		    if (instantiated_head[rule.precomputed_patterns_head[0].pos2]
			    .getValue() >= 0
			    && instantiated_head[rule.precomputed_patterns_head[1].pos2]
				    .getValue() == Schema.ALL_RESOURCES) {
			k1.values[0] = instantiated_head[rule.precomputed_patterns_head[0].pos2]
				.getValue();
			possibleBindings = g.mapping_head1_first_generic
				.get(k1);
		    } else if (instantiated_head[rule.precomputed_patterns_head[1].pos2]
			    .getValue() >= 0
			    && instantiated_head[rule.precomputed_patterns_head[0].pos2]
				    .getValue() == Schema.ALL_RESOURCES) {
			k1.values[0] = instantiated_head[rule.precomputed_patterns_head[1].pos2]
				.getValue();
			possibleBindings = g.mapping_head2_first_generic
				.get(k1);
		    } else {
			if (instantiated_head[rule.precomputed_patterns_head[0].pos2]
				.getValue() <= Schema.SET_THRESHOLD
				&& instantiated_head[rule.precomputed_patterns_head[1].pos2]
					.getValue() == Schema.ALL_RESOURCES) {

			    long idSet = instantiated_head[rule.precomputed_patterns_head[0].pos2]
				    .getValue();
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

				    supportTriple[g.pos_shared_vars_precomp_generics[0][0].pos2]
					    .setValue(shared_set_id);

				    // Copy supportTriple in patterns
				    SequencePatterns pattern = new SequencePatterns();
				    pattern.tripleOutput[0]
					    .setValue(supportTriple[0]
						    .getValue());
				    pattern.tripleOutput[1]
					    .setValue(supportTriple[1]
						    .getValue());
				    pattern.tripleOutput[2]
					    .setValue(supportTriple[2]
						    .getValue());
				    pattern.next = output.patterns;
				    output.patterns = pattern;
				    return output;
				}
			    }
			} else if (instantiated_head[rule.precomputed_patterns_head[1].pos2]
				.getValue() <= Schema.SET_THRESHOLD
				&& instantiated_head[rule.precomputed_patterns_head[0].pos2]
					.getValue() == Schema.ALL_RESOURCES) {
			    long idSet = instantiated_head[rule.precomputed_patterns_head[1].pos2]
				    .getValue();

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

				    supportTriple[g.pos_shared_vars_precomp_generics[0][0].pos2]
					    .setValue(shared_set_id);

				    // Copy supportTriple in patterns
				    SequencePatterns pattern = new SequencePatterns();
				    pattern.tripleOutput[0]
					    .setValue(supportTriple[0]
						    .getValue());
				    pattern.tripleOutput[1]
					    .setValue(supportTriple[1]
						    .getValue());
				    pattern.tripleOutput[2]
					    .setValue(supportTriple[2]
						    .getValue());
				    pattern.next = output.patterns;
				    output.patterns = pattern;
				    return output;
				}
			    }
			} else {
			    Collection<Long> col1 = Schema
				    .getInstance()
				    .getSubset(
					    instantiated_head[rule.precomputed_patterns_head[0].pos2]
						    .getValue(), context);
			    Collection<Long> col2 = Schema
				    .getInstance()
				    .getSubset(
					    instantiated_head[rule.precomputed_patterns_head[1].pos2]
						    .getValue(), context);
			    if (col1 != null && col2 != null) {
				// Calculate all the set of keys.
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

						for (int j = 0; j < sizeTuple; ++j) {
						    supportTriple[g.pos_shared_vars_precomp_generics[0][j].pos2]
							    .setValue(col
								    .getRawValues()[i]);
						}
						// Copy supportTriple in
						// patterns
						SequencePatterns pattern = new SequencePatterns();
						pattern.tripleOutput[0]
							.setValue(supportTriple[0]
								.getValue());
						pattern.tripleOutput[1]
							.setValue(supportTriple[1]
								.getValue());
						pattern.tripleOutput[2]
							.setValue(supportTriple[2]
								.getValue());
						pattern.next = output.patterns;
						output.patterns = pattern;
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
			for (int y = 0; y < possibleBindings.getNTuples(); ++y) {
			    for (int i = 0; i < g.pos_shared_vars_precomp_generics[0].length; ++i) {
				supportTriple[g.pos_shared_vars_precomp_generics[0][i].pos2]
					.setValue(possibleBindings.getValue(y,
						i));
			    }
			    // Copy supportTriple in patterns
			    SequencePatterns pattern = new SequencePatterns();
			    pattern.tripleOutput[0].setValue(supportTriple[0]
				    .getValue());
			    pattern.tripleOutput[1].setValue(supportTriple[1]
				    .getValue());
			    pattern.tripleOutput[2].setValue(supportTriple[2]
				    .getValue());
			    pattern.next = output.patterns;
			    output.patterns = pattern;
			}
			return output.patterns == null ? null : output;
		    }
		}
	    } else {
		throw new Exception("Not supported");
	    }
	}

	return null;
    }

    public static void generate_new_chains(Chain inputChain,
	    RuleChildren child, WritableContainer<Chain> outputChains,
	    ActionContext context, Tuple supportTuple, Chain supportChain)
	    throws Exception {
	generate_new_chains(inputChain, child, outputChains, context,
		supportTuple, supportChain, Integer.MAX_VALUE);
    }

    public static void generate_new_chains(Chain inputChain,
	    RuleChildren child, WritableContainer<Chain> outputChains,
	    ActionContext context, Tuple supportTuple, Chain supportChain,
	    int maximum) throws Exception {
	SequencePatterns patterns = child.patterns;
	if (patterns != null) {
	    do {
		supportTuple.set(patterns.tripleOutput);
		generate_new_chain(child.rule, child.strag_id,
			child.single_node, supportTuple, patterns.tripleOutput,
			child.current_pattern, child.head, child.ref_to_memory,
			inputChain, supportChain, outputChains, context,
			child.list_head, child.list_id);
		patterns = patterns.next;
	    } while (patterns != null && --maximum > 0);
	}
    }

    public static void generate_new_chain(Rule rule, int stratg_id,
	    boolean group_to_single_node, Tuple tuple, RDFTerm[] rawTuple,
	    int currentPattern, RDFTerm[] head, int refToMemory,
	    Chain inputChain, Chain newChain,
	    WritableContainer<Chain> outputChains, ActionContext context,
	    long list_head, int list_id) throws Exception {

	inputChain.generateChild(context, newChain);

	if (rule.type == 1) {
	    newChain.addAction(RuleExecutor1.class.getName(), tuple,
		    head[0].getValue(), head[1].getValue(), head[2].getValue(),
		    refToMemory, stratg_id, currentPattern, rule.id);
	} else if (rule.type == 2) {
	    newChain.addAction(RuleExecutor2.class.getName(), tuple,
		    head[0].getValue(), head[1].getValue(), head[2].getValue(),
		    refToMemory, stratg_id, currentPattern, rule.id);
	} else if (rule.type == 3) {
	    newChain.addAction(RuleExecutor3.class.getName(), tuple,
		    head[0].getValue(), head[1].getValue(), head[2].getValue(),
		    refToMemory, stratg_id, currentPattern, rule.id);
	} else if (rule.type == 4) {
	    newChain.addAction(RuleExecutor4.class.getName(), tuple,
		    head[0].getValue(), head[1].getValue(), head[2].getValue(),
		    refToMemory, stratg_id, currentPattern, rule.id, list_head,
		    list_id);
	}

	if (QueryPIE.ENABLE_COMPLETENESS
		&& rawTuple[0].getValue() != Schema.SCHEMA_SUBSET)
	    AddIntermediateTriples.applyTo(context, rawTuple[0].getValue(),
		    rawTuple[1].getValue(), rawTuple[2].getValue(), newChain);

	if (group_to_single_node) {
	    SendTo.applyTo(context, new Tuple(new TString(SendTo.THIS),
		    new TBoolean(true)), newChain, null);
	}

	newChain.setInputTuple(tuple);
	newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);

	outputChains.add(newChain);
    }

    @Override
    public void process(Tuple inputTuple,
	    Chain remainingChain,
	    // Action[] actionsInChain, int indexAction,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	// This method will never be called. Nothing to do here
    }
}