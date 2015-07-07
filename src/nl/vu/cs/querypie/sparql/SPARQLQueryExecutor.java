package nl.vu.cs.querypie.sparql;

import java.util.ArrayList;
import java.util.Arrays;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.joins.HashJoin;
import nl.vu.cs.querypie.joins.Table;
import nl.vu.cs.querypie.reasoner.IncrRuleBCAlgo;
import nl.vu.cs.querypie.reasoner.QSQBCAlgo;
import nl.vu.cs.querypie.reasoner.RuleBCAlgo;
import nl.vu.cs.querypie.reasoning.expand.ExpandTree;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

public class SPARQLQueryExecutor extends Action {

	public static final int LA_NAMEREST = 0;
	public static final int LA_NAMEEXIST = 1;
	public static final int LA_NAMESETS = 2;
	public static final int IA_POSSETS = 3;
	public static final int B_QSQ = 4;

	private long[] existingTable;
	private long[] remainingPatterns;
	private long[] nameSets;
	private int[] posSets;
	private boolean qsq;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(LA_NAMEREST, "LA_NAMEREST", null, false);
		conf.registerParameter(LA_NAMEEXIST, "LA_NAMEEXIST", null, false);
		conf.registerParameter(LA_NAMESETS, "LA_NAMESETS", null, false);
		conf.registerParameter(IA_POSSETS, "IA_POSSETS", null, false);
		conf.registerParameter(B_QSQ, "B_QSQ", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		existingTable = getParamLongArray(LA_NAMEEXIST);
		remainingPatterns = getParamLongArray(LA_NAMEREST);
		nameSets = getParamLongArray(LA_NAMESETS);
		posSets = getParamIntArray(IA_POSSETS);
		qsq = getParamBoolean(B_QSQ);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		// Here I receive in input the list of patterns
		remainingPatterns = new long[inputTuple.getNElements()];
		for (int i = 0; i < remainingPatterns.length; ++i) {
			remainingPatterns[i] = ((RDFTerm) inputTuple.get(i)).getValue();
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {

		if (!context.isPrincipalBranch()) {
			return;
		}

		/***** PREPARE THE PATTERN TO READ *****/
		int nvars = 0;
		ActionSequence newChain = new ActionSequence();
		RDFTerm[] t2 = { new RDFTerm(), new RDFTerm(), new RDFTerm() };
		for (int m = 0; m < 3; ++m) {
			if (remainingPatterns[m] < 0) {
				t2[m].setValue(Schema.ALL_RESOURCES);
				nvars++;
			} else {
				t2[m].setValue(remainingPatterns[m]);
			}
		}

		if (nameSets != null && nameSets.length > 0) {
			for (int i = 0; i < nameSets.length; ++i) {
				t2[posSets[i]].setValue(nameSets[i]);
			}
		}

		/***** READ THE PATTERN *****/
		// boolean incrementalExecution = nvars == 1 && nameSets != null
		// 		&& nameSets.length == 1;
                boolean incrementalExecution = false;
		if (incrementalExecution) {
			context.putObjectInCache(ExpandTree.FINISHED_EXPANSION, null);
			IncrRuleBCAlgo.applyTo(t2[0], t2[1], t2[2], posSets[0],
					nameSets[0], newChain);
		} else {
			if (qsq) {
				QSQBCAlgo.applyTo(t2[0], t2[1], t2[2], newChain);
			} else {
				RuleBCAlgo.cleanup(context);
				RuleBCAlgo.applyTo(t2[0], t2[1], t2[2], true, newChain);	
			}			
		}

		/***** COLLECT ALL THE RESULTS BEFORE WE CONTINUE WITH ANOTHER PATTERN *****/
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				RDFTerm.class.getName(), RDFTerm.class.getName(),
				RDFTerm.class.getName());
		newChain.add(c);

		/***** EITHER PERFORM A HASH JOIN OR SIMPLY RETURN ONLY THE VARIABLES *****/
		int sizeOutputTuples = 0;
		if (existingTable == null) {
			int[] posVariables = calculatePosVariables(Arrays.copyOf(
					remainingPatterns, 3));
			sizeOutputTuples = posVariables.length;
			existingTable = new long[sizeOutputTuples];
			for (int i = 0; i < sizeOutputTuples; ++i) {
				existingTable[i] = remainingPatterns[posVariables[i]];
			}

			c = ActionFactory.getActionConf(Project.class);
			c.setParamIntArray(Project.IA_POS, posVariables);
			newChain.add(c);
		} else {
			Table previousTuples = (Table) context
					.getObjectFromCache("existingTable");
			if (previousTuples == null || previousTuples.size() == 0) {
				return; // No more joins to perform, since the previous patterns
						// did not retrieve anything.
			}

			int[][] positions = calculateJoinsAndPositionsToCopy(existingTable,
					Arrays.copyOf(remainingPatterns, 3));
			int[] posToCopy = positions[2];
			int prevSize = existingTable.length;
			sizeOutputTuples = prevSize + posToCopy.length;
			existingTable = Arrays.copyOf(existingTable, sizeOutputTuples);
			for (int i = 0; i < posToCopy.length; ++i) {
				existingTable[prevSize + i] = remainingPatterns[posToCopy[i]];
			}

			c = ActionFactory.getActionConf(HashJoin.class);
			c.setParamString(HashJoin.S_TABLE, "existingTable");
			c.setParamIntArray(HashJoin.IA_POS_TABLE, positions[0]);
			c.setParamIntArray(HashJoin.IA_POS_INPUT, positions[1]);
			c.setParamIntArray(HashJoin.IA_POS_COPYINPUT, positions[2]);
			newChain.add(c);
		}

		if (remainingPatterns != null && remainingPatterns.length > 3) {
			/***** CALCULATE A NUMBER OF ACCEPTABLE VALUES FOR THE NEXT JOIN *****/
			int[][] nextJoins = calculateJoinsAndPositionsToCopy(existingTable,
					Arrays.copyOfRange(remainingPatterns, 3, 6));
			int[] futurePosJoins = nextJoins[0];
			long[] nameSets = new long[futurePosJoins.length];
			for (int i = 0; i < nameSets.length; ++i) {
				nameSets[i] = ((long) (context.getNewBucketID() * -1)) << 16;
			}
			c = ActionFactory.getActionConf(CalculateSets.class);
			c.setParamIntArray(CalculateSets.IA_POSSETS, futurePosJoins);
			c.setParamLongArray(CalculateSets.LA_NAMESETS, nameSets);
			newChain.add(c);

			/***** COPY THE RESULTS OF THE HASH IN A NEW TABLE *****/
			c = ActionFactory.getActionConf(AddToTable.class);
			c.setParamString(AddToTable.S_NAMETABLE, "existingTable");
			c.setParamInt(AddToTable.I_SIZEROW, sizeOutputTuples);
			newChain.add(c);

			/***** REPEAT THE PROCESS *****/
			c = ActionFactory.getActionConf(this.getClass());
			c.setParamLongArray(LA_NAMEREST, Arrays.copyOfRange(
					remainingPatterns, 3, remainingPatterns.length));
			c.setParamLongArray(LA_NAMEEXIST, existingTable);
			c.setParamLongArray(LA_NAMESETS, nameSets);
			c.setParamIntArray(IA_POSSETS, nextJoins[1]);
			c.setParamBoolean(B_QSQ, qsq);
			newChain.add(c);

		} else {
			// Nothing left to do. Let the final tuples being processed by the
			// next action.
		}

		actionOutput.branch(newChain);
	}

	private final int[] calculatePosVariables(long[] tuple) {
		ArrayList<Integer> positions = new ArrayList<Integer>();
		for (int i = 0; i < tuple.length; ++i) {
			if (tuple[i] < 0)
				positions.add(i);
		}

		int[] output = new int[positions.size()];
		for (int i = 0; i < output.length; ++i) {
			output[i] = positions.get(i);
		}
		return output;
	}

	private final static int[][] calculateJoinsAndPositionsToCopy(long[] set1,
			long[] set2) {
		ArrayList<Integer> pos1 = new ArrayList<Integer>();
		ArrayList<Integer> pos2 = new ArrayList<Integer>();
		ArrayList<Integer> pos3 = new ArrayList<Integer>();

		for (int i = 0; i < set2.length; ++i) {
			if (set2[i] < 0) {
				boolean found = false;
				for (int j = 0; j < set1.length; ++j) {
					if (set1[j] == set2[i]) {
						pos1.add(j);
						pos2.add(i);
						found = true;
						break;
					}
				}

				if (!found) {
					pos3.add(i);
				}
			}
		}

		int[][] results = new int[3][];
		int[] p1 = new int[pos1.size()];
		int[] p2 = new int[pos2.size()];
		for (int i = 0; i < p1.length; ++i) {
			p1[i] = pos1.get(i);
			p2[i] = pos2.get(i);
		}
		int[] p3 = new int[pos3.size()];
		for (int i = 0; i < p3.length; ++i) {
			p3[i] = pos3.get(i);
		}
		results[0] = p1;
		results[1] = p2;
		results[2] = p3;
		return results;
	}
}
