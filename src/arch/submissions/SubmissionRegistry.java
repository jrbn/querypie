package arch.submissions;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.StatisticsCollector;
import arch.actions.Action;
import arch.actions.ActionsProvider;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.net.NetworkLayer;
import arch.storage.Container;
import arch.storage.Factory;
import arch.utils.Configuration;
import arch.utils.Consts;

public class SubmissionRegistry {

	static final Logger log = LoggerFactory.getLogger(SubmissionRegistry.class);

	Factory<Chain> chainFactory = new Factory<Chain>(Chain.class);
	Factory<Submission> submissionFactory = new Factory<Submission>(
			Submission.class);

	ActionsProvider ap;
	DataProvider dp;
	StatisticsCollector stats;
	Buckets buckets;
	NetworkLayer net;
	Configuration conf;

	Map<Integer, Submission> submissions = new HashMap<Integer, Submission>();
	Container<Chain> chainsToResolve;
	int submissionCounter = 0;

	public SubmissionRegistry(NetworkLayer net, StatisticsCollector stats,
			Container<Chain> chainsToResolve, Buckets buckets,
			ActionsProvider ap, DataProvider dp, Configuration conf) {
		this.net = net;
		this.stats = stats;
		this.chainsToResolve = chainsToResolve;
		this.buckets = buckets;
		this.ap = ap;
		this.dp = dp;
		this.conf = conf;
	}

	public void updateCounters(int submissionId, long chainId,
			long parentChainId, int nchildren, int repFactor) {
		Submission sub = getSubmission(submissionId);
		if (log.isDebugEnabled()) {
			log.debug("updateCounters: submissionId = " + submissionId
					+ ", chainId = " + chainId + ", parentChainId = "
					+ parentChainId + ", nchildren = " + nchildren
					+ ", repFactor = " + repFactor);
		}

		synchronized (sub) {
			if (nchildren > 0) { // Set the expected children in the
				// map
				int[] c = sub.childrens.get(chainId);
				if (c == null) {
					c = new int[2];
					sub.childrens.put(chainId, c);
				}
				c[0] += nchildren;

				if (c[0] == c[1] && c[0] == 0)
					sub.childrens.remove(chainId);
			}

			if (parentChainId == -1) { // It is one of the root chains
				sub.rootChainsReceived += repFactor;
				if (repFactor == 0)
					sub.rootChainsReceived--;
			} else {
				// Change the children field of the parent chain
				int[] c = sub.childrens.get(parentChainId);
				if (c == null) {
					c = new int[2];
					sub.childrens.put(parentChainId, c);
				}

				if (repFactor > 0) {
					c[0]--; // Got a child
					c[1] += repFactor - 1;
				} else {
					c[1]--;
				}
				if (c[0] == c[1] && c[0] == 0)
					sub.childrens.remove(parentChainId);
			}

			if (sub.rootChainsReceived == 0 && sub.childrens.size() == 0) {
				if (sub.assignedBucket != -1) {
					Bucket bucket = buckets.getExistingBucket(submissionId,
							sub.assignedBucket);
					bucket.waitUntilFinished();
				}

				sub.state = Consts.STATE_FINISHED;
				sub.endTime = System.nanoTime();
				sub.notifyAll();
			}
			sub.chainProcessed++;
		}

		log.debug("Updated after chain " + chainId + "(" + parentChainId
				+ ") c=" + nchildren + " r=" + repFactor + " finished: rcr: "
				+ sub.rootChainsReceived + " cs: " + sub.childrens.size());
	}

	public Submission getSubmission(int submissionId) {
		return submissions.get(submissionId);
	}

	private Submission submitNewJob(Context context, JobDescriptor job)
			throws Exception {

		ActionContext ac = new ActionContext(context, context.getDataProvider());

		Chain chain = chainFactory.get();

		// Add Dynamic Actions
		if (job.availableRules != null && job.availableRules.length() > 0) {
			String[] rs = job.availableRules.split(",");
			chain.init(rs);
		} else {
			chain.init(null);
		}

		chain.setParentChainId(-1);
		// Add actions
		for (int i = job.action.size() - 1; i >= 0; i--) {
			Action action = ap.get(job.action.get(i));
			chain = action.apply(ac, job.tuples.get(i), chain, null, null);
			ap.release(action);
		}

		chain.setInputLayerId(job.inputLayer);
		chain.setInputTuple(job.inputTuple);
		chain.setExcludeExecution(job.getExcludeMainChain());

		Submission sub = submissionFactory.get();
		sub.init();
		sub.startupTime = System.nanoTime();
		synchronized (this) {
			sub.submissionId = submissionCounter++;
		}
		sub.state = Consts.STATE_OPEN;
		sub.finalStatsReceived = 0;
		sub.rootChainsReceived = -1;
		sub.chainProcessed = 0;
		sub.printIntermediateStats = job.printIntermediateStats;
		sub.printStats = job.printStatistics;
		sub.assignedBucket = job.assignedBucket;

		submissions.put(sub.submissionId, sub);
		chain.setSubmissionNode(context.getNetworkLayer().getMyPartition());
		chain.setSubmissionId(sub.submissionId);

		chainsToResolve.add(chain);
		chainFactory.release(chain);

		return sub;
	}

	public void releaseSubmission(Submission submission) {
		submissions.remove(submission.submissionId);
		submissionFactory.release(submission);
	}

	public void setState(int submissionId, String state) {
		submissions.get(submissionId).state = state;
	}

	public void cleanupSubmission(Submission submission) throws IOException,
			InterruptedException {

		for (int i = 0; i < net.getNumberNodes(); ++i) {
			WriteMessage msg = net.getMessageToSend(net.getPeerLocation(i),
					NetworkLayer.nameMgmtReceiverPort);
			msg.writeByte((byte) 8);
			msg.writeInt(submission.getSubmissionId());
			msg.finish();
		}
	}

	public Submission waitForCompletion(Context context, JobDescriptor job)
			throws Exception {

		Submission submission = submitNewJob(context, job);
		int printInterval = conf.getInt(Consts.STATISTICAL_INTERVAL,
				Consts.DEFAULT_STATISTICAL_INTERVAL);
		// Pool the submission registry to know when it is present and return it
		synchronized (submission) {
			while (!submission.getState().equalsIgnoreCase(
					Consts.STATE_FINISHED)) {
				submission.wait(printInterval);
				if (submission.printIntermediateStats
						&& !submission.getState().equalsIgnoreCase(
								Consts.STATE_FINISHED))
					stats.printStatistics(submission.getSubmissionId());
			}
		}

		// Clean up the eventual things going on in the nodes
		cleanupSubmission(submission);
		return submission;
	}

	public void getStatistics(JobDescriptor job, Submission submission) {
		if (job.waitForStatistics) {
			try {
				log.info("Waiting for statistics...");
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// ignore
			}
		}

		submission.counters = stats.removeCountersSubmission(submission
				.getSubmissionId());

		// Print the counters
		if (submission.printStats) {
			String stats = "Final statistics submission "
					+ submission.getSubmissionId() + ":\n";
			if (submission.counters != null) {
				for (Map.Entry<String, Long> entry : submission.counters
						.entrySet()) {
					stats += " " + entry.getKey() + " = " + entry.getValue()
							+ "\n";
				}
			}
			log.info(stats);
		}
	}
}