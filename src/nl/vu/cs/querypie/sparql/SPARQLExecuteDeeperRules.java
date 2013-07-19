package nl.vu.cs.querypie.sparql;

import java.util.Set;

import nl.vu.cs.querypie.QueryPIE;
import nl.vu.cs.querypie.reasoner.RuleBCAlgo;
import nl.vu.cs.querypie.reasoner.RuleGetPattern;
import nl.vu.cs.querypie.reasoner.Ruleset;
import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.executors.TreeExpander;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.MultiValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.actions.FilterDuplicates;
import arch.actions.SendTo;
import arch.chains.Chain;
import arch.data.types.TBoolean;
import arch.data.types.TInt;
import arch.data.types.TLong;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.TupleComparator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class SPARQLExecuteDeeperRules extends Action {

    static final Logger log = LoggerFactory
	    .getLogger(SPARQLExecuteDeeperRules.class);

    int position, oldSize;
    boolean active;
    private RDFTerm[] t2 = { new RDFTerm(), new RDFTerm(), new RDFTerm() };

    public static Chain applyTo(ActionContext context, Tuple tuple,
	    Chain chain, WritableContainer<Chain> childrenChains)
	    throws Exception {
	TLong s = new TLong();
	TLong p = new TLong();
	TLong o = new TLong();
	TInt pos = new TInt();
	tuple.get(s, p, o, pos);
	chain.addAction(SPARQLExecuteDeeperRules.class.getName(), tuple,
		s.getValue(), p.getValue(), o.getValue(), pos.getValue());
	return chain;
    }

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	return SPARQLExecuteDeeperRules.applyTo(context, tuple, chain,
		chainsToResolve);
    }

    int count = 0;

    @SuppressWarnings("unchecked")
    @Override
    public void startProcess(ActionContext context, Chain chain,
	    Object... params) throws Exception {
	t2[0].setValue((Long) params[0]);
	t2[1].setValue((Long) params[1]);
	t2[2].setValue((Long) params[2]);
	position = (Integer) params[3];
	customSet = (Set<Long>) context.getObjectFromCache(t2[position]
		.getValue());
	oldSize = customSet.size();
	active = (Boolean) context.getObjectFromCache("sparql.incr.active");
	count = 0;
    }

    Chain newChain = new Chain();
    Tuple tuple = new Tuple();
    Set<Long> customSet;
    RDFTerm term = new RDFTerm();

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess,
	    WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	// Replace the custom set
	if (active) {
	    count++;
	    inputTuple.get(term, position);
	    customSet.remove(term.getValue());
	}
    }

    @Override
    public void stopProcess(ActionContext context, Chain chain,
	    WritableContainer<Tuple> output,
	    WritableContainer<Chain> newChains,
	    WritableContainer<Chain> chainsToSend) throws Exception {

	boolean canStop = customSet.size() == 0;

	if (!canStop && active) {

	    if (customSet.size() != oldSize) { // The set has changed. Need to
		// broadcast it
		log.debug("Broadcast set " + t2[position].getValue() + " of "
			+ customSet.size() + " elements");
		context.broadcastCacheObjects(t2[position].getValue());
	    }

	    chain.generateChild(context, newChain);
	    newChain.setExcludeExecution(false);

	    /***** CHECK WHETHER WE NEED TO REPEAT THE CYCLE *****/
	    int bucket = context.getNewBucketID();
	    SPARQLCheckBindings.applyTo(context, new Tuple(new TInt(bucket),
		    t2[0], t2[1], t2[2], new TInt(position)), newChain, null);

	    /***** SEND THE BINDINGS TO ONE BUCKET *****/
	    tuple.set(new TString(SendTo.THIS), new TBoolean(false), new TInt(
		    bucket), new TBoolean(true));
	    SendTo.applyTo(context, tuple, newChain, null);

	    /***** FILTER THE DUPLICATES *****/
	    FilterDuplicates.applyTo(context, tuple, newChain, null);

	    /***** SEND TO MULTIPLE NODE *****/
	    tuple.set(new TString(SendTo.MULTIPLE), new TBoolean(true),
		    new TInt(context.getNewBucketID()), new TBoolean(false),
		    new TString(TupleComparator.class.getName()));
	    SendTo.applyTo(context, tuple, newChain, null);

	    /***** PARTITION THE TUPLES ******/
	    TriplePartitioner.applyTo(context, null, newChain, null);

	    /***** READ TRIPLE IN INPUT *****/
	    tuple.set(t2);
	    newChain.setInputTuple(tuple);
	    newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);	    

	    /***** CHECK IF THERE ARE OTHER CHEAP RULES *****/
	    boolean first = (Boolean) context
		    .getObjectFromCache("sparql.incr.isFirst");
	    int indexRule = (Integer) context
		    .getObjectFromCache("sparql.incr.currentRule");
	    TreeExpander.RuleChildren children = (TreeExpander.RuleChildren) context
		    .getObjectFromCache("sparql.incr.children");

	    // Remove the first and check whether there is the second
	    for(int i = 0; i < TreeExpander.CONCURRENT_ACTIONS_BFS && children.patterns != null; ++i) {
		children.patterns = children.patterns.next;
	    }
	    int startIndex = indexRule;
	    if (children.patterns == null) {
		children = null;
		// Go to the next rule
		Ruleset ruleset = Ruleset.getInstance();
		Rule1[] rules1 = ruleset.getAllActiveFirstTypeRules();
		startIndex = indexRule + 1;
		for (; startIndex < rules1.length && first; ++startIndex) {
		    Rule1 rule = rules1[startIndex];
		    if (!TreeExpander.checkHead(rule, t2, context)) {
			continue;
		    }
		    children = TreeExpander.applyRuleFirstType(rule, t2);
		    if (children != null) {
			break;
		    }

		}
		if (children == null) {
		    if (first) {
			startIndex = 0;
			first = false;
		    }		    
		    Rule2[] rules2 = ruleset.getAllActiveSecondTypeRules();

		    RDFTerm[] supportTriple = { new RDFTerm(), new RDFTerm(),
			    new RDFTerm() };
		    MultiValue k1 = new MultiValue(new long[1]);
		    MultiValue k2 = new MultiValue(new long[2]);

		    for (; startIndex < rules2.length; ++startIndex) {
			Rule2 rule = rules2[startIndex];
			if (!TreeExpander.checkHead(rule, t2, context)) {
			    continue;
			}
			children = TreeExpander.applyRuleWithGenerics(rule,
				rule.GENERICS_STRATS, t2, context,
				supportTriple, k1, k2);
			if (children != null) {
			    break;
			}
		    }
		}
	    }

	    if (children != null) {
		log.debug("Try new possibilities...");
		context.putObjectInCache("sparql.incr.isFirst", first);
		context.putObjectInCache("sparql.incr.currentRule", startIndex);
		context.putObjectInCache("sparql.incr.active", true);
		context.putObjectInCache("sparql.incr.children", children);

		boolean originalCompleteness = QueryPIE.ENABLE_COMPLETENESS;
		QueryPIE.ENABLE_COMPLETENESS = false;
		TreeExpander.generate_new_chains(newChain, children,
			chainsToSend, context, tuple, new Chain(), TreeExpander.CONCURRENT_ACTIONS_BFS);
		QueryPIE.ENABLE_COMPLETENESS = originalCompleteness;
		
		newChain.setExcludeExecution(true);
		newChain.setInputLayerId(Consts.DUMMY_INPUT_LAYER_ID);
		chainsToSend.add(newChain);

	    } else { // Time to call the complete reasoning procedure
		log.debug("Invoke complete reasoning");
		context.putObjectInCache("sparql.incr.active", false);
		RuleBCAlgo.cleanup(context);
		if (QueryPIE.ENABLE_COMPLETENESS) {
		    RemoveLastElement.applyTo(context, null, newChain);
		    RuleBCAlgo.applyTo(context, tuple, newChain);
		} else {
		    RuleGetPattern.applyTo(context, tuple, newChain);
		}
		newChain.setInputTuple(tuple);
		newChain.setInputLayerId(Consts.DEFAULT_INPUT_LAYER_ID);		
		newChain.setExcludeExecution(true);
		newChains.add(newChain);
	    }
	}
    }
}
