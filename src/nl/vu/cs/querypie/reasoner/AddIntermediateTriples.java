package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.memory.InMemoryTripleContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddIntermediateTriples extends Action {

	static final Logger log = LoggerFactory
			.getLogger(AddIntermediateTriples.class);

	// Fields of the tuple parameter.
	public static final int L_FIELD1 = 0;
	public static final int L_FIELD2 = 1;
	public static final int L_FIELD3 = 2;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(L_FIELD1, "L_FIELD1", 0, true);
		conf.registerParameter(L_FIELD2, "L_FIELD2", 0, true);
		conf.registerParameter(L_FIELD3, "L_FIELD3", 0, true);
	}

	public static void applyTo(long s, long p, long o, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(AddIntermediateTriples.class);
		c.setParamLong(L_FIELD1, s);
		c.setParamLong(L_FIELD2, p);
		c.setParamLong(L_FIELD3, o);
		actions.add(c);
	}

	InMemoryTripleContainer set = new InMemoryTripleContainer();
	InMemoryTripleContainer input = null;
	InMemoryTripleContainer alreadyExplicit = null;
	InMemoryTripleContainer explicitSet = new InMemoryTripleContainer();

	long[] query = new long[3];

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// count = 0;
		input = (InMemoryTripleContainer) context
				.getObjectFromCache("inputIntermediateTuples");
		set.clear();

		alreadyExplicit = (InMemoryTripleContainer) context
				.getObjectFromCache("explicitIntermediateTuples");
		explicitSet.clear();

		query[0] = getParamLong(L_FIELD1);
		query[1] = getParamLong(L_FIELD2);
		query[2] = getParamLong(L_FIELD3);
		set.addQuery(query[0], query[1], query[2], context, input);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		RDFTerm a = (RDFTerm) inputTuple.get(0);
		RDFTerm b = (RDFTerm) inputTuple.get(1);
		RDFTerm c = (RDFTerm) inputTuple.get(2);
		if (inputTuple.getNElements() == 4
				&& ((TBoolean) inputTuple.get(3)).getValue()) {
			if (set.addTriple(a, b, c, input)) {
				// count++;
				output.output(inputTuple);
			}
		} else { // Explicit tuple
			explicitSet.addTriple(a, b, c, alreadyExplicit, input);
			output.output(inputTuple);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output) {

		// context.incrCounter("intermediate tuples", count);

		// log.debug("This action" + Arrays.toString(query) + " has added " +
		// count + " new triples");

		if (set.isToCopy()) {
			InMemoryTripleContainer current = (InMemoryTripleContainer) context
					.getObjectFromCache("intermediateTuples");
			if (current == null) {
				synchronized (InMemoryTripleContainer.class) {
					current = (InMemoryTripleContainer) context
							.getObjectFromCache("intermediateTuples");
					if (current == null) {
						current = set;
						set = new InMemoryTripleContainer();
						context.putObjectInCache("intermediateTuples", current);
					} else {
						current.addAll(set);
					}
				}
			} else {
				current.addAll(set);
			}
		}

		if (explicitSet.size() > 0) {
			if (alreadyExplicit == null) {
				synchronized (InMemoryTripleContainer.class) {
					alreadyExplicit = (InMemoryTripleContainer) context
							.getObjectFromCache("explicitIntermediateTuples");
					if (alreadyExplicit == null) {
						alreadyExplicit = explicitSet;
						explicitSet = new InMemoryTripleContainer();
						context.putObjectInCache("explicitIntermediateTuples",
								alreadyExplicit);
					} else {
						alreadyExplicit.addAll(explicitSet);
						explicitSet.clear();
					}
				}
			} else {
				alreadyExplicit.addAll(explicitSet);
				explicitSet.clear();
			}
		}

		// Clear what needs to be cleared, this object will be cached. --Ceriel
		set.clear();
		alreadyExplicit = null;
		input = null;
	}
}
