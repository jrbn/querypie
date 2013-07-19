package nl.vu.cs.querypie.sparql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.querypie.storage.disk.RDFStorage;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.TLong;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class SPARQLParser extends Action {

    static final Logger log = LoggerFactory.getLogger(SPARQLParser.class);

    @Override
    public Chain apply(ActionContext context, Tuple tuple, Chain chain,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToSend) throws Exception {
	chain.addAction(this, null, (Object[]) null);
	return chain;
    }

    @Override
    public void process(Tuple inputTuple, Chain remainingChain,
	    // Action[] actionsInChain, int indexAction,
	    WritableContainer<Chain> chainsToResolve,
	    WritableContainer<Chain> chainsToProcess, WritableContainer<Tuple> output, ActionContext context)
	    throws Exception {
	try {
	    TString tquery = new TString();
	    inputTuple.get(tquery, 0);
	    String query = tquery.getValue();
	    org.openrdf.query.parser.sparql.SPARQLParser parser = new org.openrdf.query.parser.sparql.SPARQLParser();
	    ParsedQuery q = parser.parseQuery(query, "http://www.vu.nl/");

	    TupleExpr expr = q.getTupleExpr();

	    List<StatementPattern> list = new ArrayList<StatementPattern>();
	    expr.visit(new QueryVisitor(list));

	    // Replace URIs with the numbers from the dictionary table
	    Map<String, Long> map = ((RDFStorage) context.getInputLayer(0))
		    .getCacheURLs();

	    for (StatementPattern sp : list) {
		// log.debug("SP: " + sp);
		for (Var var : sp.getVarList()) {
		    if (var.getValue() != null) {
			String value = var.getValue().stringValue();
			if (var.getValue() instanceof LiteralImpl) {
			    value = "\"" + value + "\"";
			} else {
			    value = "<" + value + ">";
			}
			if (map.containsKey(value)) {
			    var.setValue(new SesameValue(map.get(value)));
			} else {
			    log.warn("Value not found in the cache: " + value);
			}
		    }
		}
	    }

	    // // Serialize the list of patterns in a list of triples
	    long counter = -100;
	    Map<String, Long> maps = new HashMap<String, Long>();
	    long[] serializedJoin = new long[list.size() * 3];
	    int i = 0;
	    for (StatementPattern s : list) {
		Var var = s.getSubjectVar();
		if (var.getValue() != null) {
		    serializedJoin[i * 3] = ((SesameValue) var.getValue()).value;
		} else {
		    if (!maps.containsKey(var.getName())) {
			maps.put(var.getName(), --counter);
		    }
		    serializedJoin[i * 3] = maps.get(var.getName());
		}

		var = s.getPredicateVar();
		if (var.getValue() != null) {
		    serializedJoin[i * 3 + 1] = ((SesameValue) var.getValue()).value;
		} else {
		    if (!maps.containsKey(var.getName())) {
			maps.put(var.getName(), --counter);
		    }
		    serializedJoin[i * 3 + 1] = maps.get(var.getName());
		}

		var = s.getObjectVar();
		if (var.getValue() != null) {
		    serializedJoin[i * 3 + 2] = ((SesameValue) var.getValue()).value;
		} else {
		    if (!maps.containsKey(var.getName())) {
			maps.put(var.getName(), --counter);
		    }
		    serializedJoin[i * 3 + 2] = maps.get(var.getName());
		}
		++i;
	    }

	    // Output the list of triples
	    TLong[] serializedList = new TLong[list.size() * 3];
	    for (i = 0; i < serializedJoin.length; ++i) {
		serializedList[i] = new TLong();
		serializedList[i].setValue(serializedJoin[i]);
	    }

	    inputTuple.set(serializedList);
	    output.add(inputTuple);
	} catch (Exception e) {
	    log.error("Error", e);
	}
    }
}
