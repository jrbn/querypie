package arch.submissions;

import java.util.HashMap;
import java.util.Map;

public class Submission {

	long startupTime;
	long endTime;
	int submissionId;
	int finalStatsReceived;
	String state;
	int rootChainsReceived = -1;
	int chainProcessed = 0;
	Map<String, Long> counters = null;
	Map<Long, int[]> childrens = new HashMap<Long, int[]>();
	int assignedBucket = -1;
	boolean printStats;
	boolean printIntermediateStats;

	public double getExecutionTimeInMs() {
		return (double) (endTime - startupTime) / 1000 / 1000;
	}

	public int getAssignedBucket() {
		return assignedBucket;
	}
	
	public int getFinalStatsReceived() {
		return finalStatsReceived;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public int getSubmissionId() {
		return submissionId;
	}

	public String getState() {
		return state;
	}

	public int getChainsProcessed() {
		return chainProcessed;
	}

	public void init() {
		rootChainsReceived = -1;
		chainProcessed = 0;
		childrens.clear();
		assignedBucket = -1;		
	}
}