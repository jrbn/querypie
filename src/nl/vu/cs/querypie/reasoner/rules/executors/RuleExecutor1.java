package nl.vu.cs.querypie.reasoner.rules.executors;

import java.util.Collection;

import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.memory.CollectionTuples;
import nl.vu.cs.querypie.storage.memory.MultiValue;
import nl.vu.cs.querypie.storage.memory.SortedCollectionTuples;
import nl.vu.cs.querypie.storage.memory.TupleMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.SimpleData;
import arch.data.types.TBoolean;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class RuleExecutor1 extends Action {

    static final Logger log = LoggerFactory.getLogger(RuleExecutor1.class);

    /***** DATA STRUCTURES OF SUPPORT *****/
    protected Ruleset ruleset = Ruleset.getInstance();
    protected Tuple tuple = new Tuple();
    protected RDFTerm[] triple = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
    protected SimpleData[] inferredTriple = new SimpleData[4];
    protected RDFTerm[] instantiated_head = { new RDFTerm(), new RDFTerm(),
	    new RDFTerm() };

    // Contains the details of the rule
    private Rule1 ruleDef;

    // Used to filter eventual duplicates
    private boolean isHeadPrecomp;
    private int[] pos_var_head;
    private SortedCollectionTuples explicitHeadValues;
    private final MultiValue head_vars = new MultiValue();

    protected MultiValue k1 = new MultiValue(new long[1]);
    protected MultiValue k2 = new MultiValue(new long[2]);

    public RuleExecutor1() {
	inferredTriple[0] = triple[0];
	inferredTriple[1] = triple[1];
	inferredTriple[2] = triple[2];
	inferredTriple[3] = new TBoolean(true);
    }

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain inputChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	throw new Exception("Should never be called");
    }

    protected void check_head_is_precomputed(Rule1 ruleDef) {
	// Check if the rule has a precomputed head
	isHeadPrecomp = ruleDef.isHeadPrecomputed;
	if (isHeadPrecomp) {
	    explicitHeadValues = ruleDef.explicitBindings;
	    pos_var_head = ruleDef.HEAD.getPositionVars();
	    head_vars.setNumberFields(pos_var_head.length);
	}
    }

    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {

	ruleDef = ruleset.getRuleFirstType((Integer) params[6]);

	check_head_is_precomputed(ruleDef);

	// Instantiate the output
	for (int i = 0; i < 3; ++i) {
	    instantiated_head[i].setValue((Long) params[i]);
	    triple[i].setValue(ruleDef.HEAD.p[i].getValue());
	}
    }

    public void outputTuple(RDFTerm[] triple, WritableContainer<Tuple> output)
	    throws Exception {
	if (isHeadPrecomp) {
	    for (int i = 0; i < pos_var_head.length; ++i) {
		head_vars.values[i] = triple[pos_var_head[i]].getValue();
	    }
	    if (explicitHeadValues.contains(head_vars)) {
		return;
	    }
	}
	tuple.set(inferredTriple);
	output.add(tuple);
    }

    protected void process_precomputed(Rule1 ruleDef, Tuple inputTuple,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	// How many variables are shared?
	if (ruleDef.precomputed_patterns_head.length == 1) {
	    RDFTerm term = instantiated_head[ruleDef.precomputed_patterns_head[0].pos2];
	    Collection<Long> values = ruleDef.precomputed_tuples.getAllValues(
		    ruleDef.precomputed_patterns_head[0].pos1, true);
	    if (term.getValue() == Schema.ALL_RESOURCES) {
		// Output all values
		for (long value : values) {
		    triple[ruleDef.precomputed_patterns_head[0].pos2]
			    .setValue(value);
		    outputTuple(triple, output);
		}
	    } else if (term.getValue() >= 0) {
		if (values.contains(term.getValue())) {
		    outputTuple(triple, output);
		}
	    } else {
		Collection<Long> col = Schema.getInstance().getSubset(
			term.getValue(), context);
		for (long value : col) {
		    if (values.contains(value)) {
			triple[ruleDef.precomputed_patterns_head[0].pos2]
				.setValue(value);
			outputTuple(triple, output);
		    }
		}
	    }
	} else if (ruleDef.precomputed_patterns_head.length == 2) {
	    long value1 = instantiated_head[ruleDef.precomputed_patterns_head[0].pos2]
		    .getValue();
	    long value2 = instantiated_head[ruleDef.precomputed_patterns_head[1].pos2]
		    .getValue();
	    if (value1 >= 0) {
		if (value2 >= 0) {
		    int[] pos = new int[2];
		    pos[0] = ruleDef.precomputed_patterns_head[0].pos1;
		    pos[1] = ruleDef.precomputed_patterns_head[1].pos1;
		    SortedCollectionTuples values = ruleDef.precomputed_tuples
			    .getAllValues(pos);
		    k2.values[0] = value1;
		    k2.values[1] = value2;
		    if (values.contains(k2)) {
			triple[ruleDef.precomputed_patterns_head[0].pos2]
				.setValue(value1);
			triple[ruleDef.precomputed_patterns_head[1].pos2]
				.setValue(value2);
			outputTuple(triple, output);
		    }
		} else {
		    int[] in = new int[1];
		    in[0] = ruleDef.precomputed_patterns_head[0].pos1;
		    int[] out = new int[1];
		    out[0] = ruleDef.precomputed_patterns_head[1].pos1;

		    TupleMap col = ruleDef.precomputed_tuples
			    .getBindingsFromBindings(in, out, true);
		    if (col != null) {
			k1.values[0] = value1;
			CollectionTuples tuples = col.get(k1);
			if (tuples != null) {
			    triple[ruleDef.precomputed_patterns_head[0].pos2]
				    .setValue(value1);
			    if (value2 == Schema.ALL_RESOURCES) {
				for (int y = 0; y < tuples.getNTuples(); ++y) {
				    triple[ruleDef.precomputed_patterns_head[1].pos2]
					    .setValue(tuples.getValue(y, 0));
				    outputTuple(triple, output);
				}
			    } else {
				Collection<Long> set = Schema.getInstance()
					.getSubset(value2, context);
				if (set != null) {
				    long[] rawValues = tuples.getRawValues();
				    for (int y = tuples.getStart(); y < tuples
					    .getEnd(); ++y) {
					long value = rawValues[y];
					if (set.contains(value)) {
					    triple[ruleDef.precomputed_patterns_head[1].pos2]
						    .setValue(value);
					    outputTuple(triple, output);
					}
				    }
				}
			    }
			}
		    }
		}
		return;
	    }

	    if (value2 >= 0) {
		int[] in = new int[1];
		in[0] = ruleDef.precomputed_patterns_head[1].pos1;
		int[] out = new int[1];
		out[0] = ruleDef.precomputed_patterns_head[0].pos1;

		TupleMap col = ruleDef.precomputed_tuples
			.getBindingsFromBindings(in, out, true);
		if (col != null) {
		    k1.values[0] = value2;
		    CollectionTuples tuples = col.get(k1);
		    if (tuples != null) {
			triple[ruleDef.precomputed_patterns_head[1].pos2]
				.setValue(value2);
			if (value1 == Schema.ALL_RESOURCES) {
			    for (int y = 0; y < tuples.getNTuples(); ++y) {
				triple[ruleDef.precomputed_patterns_head[0].pos2]
					.setValue(tuples.getValue(y, 0));
				outputTuple(triple, output);
			    }
			} else {
			    Collection<Long> set = Schema.getInstance()
				    .getSubset(value1, context);
			    if (set != null) {
				long[] rawValues = tuples.getRawValues();
				for (int y = tuples.getStart(); y < tuples
					.getEnd(); ++y) {
				    long value = rawValues[y];
				    if (set.contains(value)) {
					triple[ruleDef.precomputed_patterns_head[1].pos2]
						.setValue(value);
					outputTuple(triple, output);
				    }
				}
			    }
			}
		    }
		}
	    } else { // Both are either -1 or a subset

		int[] pos = new int[2];
		pos[0] = ruleDef.precomputed_patterns_head[0].pos1;
		pos[1] = ruleDef.precomputed_patterns_head[1].pos1;
		SortedCollectionTuples values = ruleDef.precomputed_tuples
			.getAllValues(pos);

		Collection<Long> set1 = null;
		Collection<Long> set2 = null;
		if (value1 != Schema.ALL_RESOURCES) {
		    set1 = Schema.getInstance().getSubset(value1, context);
		}
		if (value2 != Schema.ALL_RESOURCES) {
		    set2 = Schema.getInstance().getSubset(value2, context);
		}

		if (value1 != Schema.ALL_RESOURCES
			&& value2 == Schema.ALL_RESOURCES) {
		    MultiValue value = new MultiValue(new long[2]);
		    for (int i = 0; i < values.size(); ++i) {
			values.get(value, i);
			if (set1.contains(value.values[0])) {
			    triple[ruleDef.precomputed_patterns_head[0].pos2]
				    .setValue(value.values[0]);
			    triple[ruleDef.precomputed_patterns_head[1].pos2]
				    .setValue(value.values[1]);
			    outputTuple(triple, output);
			}
		    }
		} else if (value1 == -1 && value2 != -1) {
		    MultiValue value = new MultiValue(new long[2]);
		    for (int i = 0; i < values.size(); ++i) {
			values.get(value, i);
			if (set2.contains(value.values[1])) {
			    triple[ruleDef.precomputed_patterns_head[0].pos2]
				    .setValue(value.values[0]);
			    triple[ruleDef.precomputed_patterns_head[1].pos2]
				    .setValue(value.values[1]);
			    outputTuple(triple, output);
			}
		    }
		} else {
		    MultiValue value = new MultiValue(new long[2]);
		    for (int i = 0; i < values.size(); ++i) {
			values.get(value, i);
			triple[ruleDef.precomputed_patterns_head[0].pos2]
				.setValue(value.values[0]);
			triple[ruleDef.precomputed_patterns_head[1].pos2]
				.setValue(value.values[1]);
			outputTuple(triple, output);
		    }
		}
	    }
	} else {
	    throw new Exception("Not supported");
	}
    }

    @Override
    public void process(Tuple inputTuple,
	    Chain remainingChain,
	    // Action[] actionsInChain, int indexAction,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	process_precomputed(ruleDef, inputTuple, output, context);

    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output,
	    WritableContainer<Chain> newChains,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	explicitHeadValues = null;
	isHeadPrecomp = false;
    }
}
