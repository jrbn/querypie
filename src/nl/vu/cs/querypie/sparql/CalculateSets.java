package nl.vu.cs.querypie.sparql;

import java.util.Set;
import java.util.TreeSet;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.storage.RDFTerm;

public class CalculateSets extends Action {

	public static final int IA_POSSETS = 0;
	public static final int LA_NAMESETS = 1;

	private int nsets;
	private int[] posSets;
	private Set<Long> s1, s2, s3;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(IA_POSSETS, "IA_POSSETS", null, true);
		conf.registerParameter(LA_NAMESETS, "LA_NAMESETS", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		posSets = getParamIntArray(IA_POSSETS);
		nsets = posSets.length;

		s1 = new TreeSet<Long>();
		if (nsets > 1) {
			s2 = new TreeSet<Long>();
		}
		if (nsets > 2) {
			s3 = new TreeSet<Long>();
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		s1.add(((RDFTerm)tuple.get(posSets[0])).getValue());
		if (nsets > 1) {
			s2.add(((RDFTerm)tuple.get(posSets[1])).getValue());
		}
		if (nsets > 2) {
			s3.add(((RDFTerm)tuple.get(posSets[2])).getValue());
		}
		actionOutput.output(tuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		//Store them
		long[] nameSets = getParamLongArray(LA_NAMESETS);
		switch(nsets) {
		case 1:
			context.putObjectInCache(nameSets[0], s1);
			context.broadcastCacheObjects(nameSets[0]);
			break;
		case 2:
			context.putObjectInCache(nameSets[0], s1);
			context.putObjectInCache(nameSets[1], s2);
			context.broadcastCacheObjects(nameSets[0], nameSets[1]);
			break;
		case 3:
			context.putObjectInCache(nameSets[0], s1);
			context.putObjectInCache(nameSets[1], s2);
			context.putObjectInCache(nameSets[2], s3);
			context.broadcastCacheObjects(nameSets[0], nameSets[1], nameSets[2]);
		}
		
		posSets = null;
		s1 = s2 = s3 = null;
	}
}
