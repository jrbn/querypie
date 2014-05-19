package nl.vu.cs.querypie.sparql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.querypie.storage.disk.RDFStorage;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPARQLParser extends Action {

    static final Logger log = LoggerFactory.getLogger(SPARQLParser.class);

    @Override
    public void process(Tuple inputTuple, ActionContext context,
	    ActionOutput output) throws Exception {
	try {
	    TString tquery = (TString) inputTuple.get(0);
	    String query = tquery.getValue();
	    org.openrdf.query.parser.sparql.SPARQLParser parser = new org.openrdf.query.parser.sparql.SPARQLParser();
	    ParsedQuery q = parser.parseQuery(query, "http://www.vu.nl/");

            if (log.isDebugEnabled()) {
                log.debug("ParsedQuery = " + q);
            }   

	    TupleExpr expr = q.getTupleExpr();
            if (log.isDebugEnabled()) {
                log.debug("TupleExpr = " + expr);
            }

	    List<StatementPattern> list = new ArrayList<StatementPattern>();
	    expr.visit(new QueryVisitor(list));

	    // Replace URIs with the numbers from the dictionary table
	    Map<String, Long> map = ((RDFStorage) context.getContext()
		    .getInputLayer(InputLayer.DEFAULT_LAYER)).getCacheURLs();

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
                if (log.isInfoEnabled()) {
                    log.info("line: " + serializedJoin[i * 3] + ", " + serializedJoin[i * 3 + 1] + ", " + serializedJoin[i * 3 + 2]);
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
	    output.output(inputTuple);
	} catch (Exception e) {
	    log.error("Error", e);
	}
    }
}
