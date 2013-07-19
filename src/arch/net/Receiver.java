package arch.net;

import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.RemoteCodeExecutor;
import arch.StatisticsCollector;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.Container;
import arch.storage.Factory;
import arch.storage.container.WritableContainer;
import arch.submissions.JobDescriptor;
import arch.submissions.Submission;
import arch.utils.Utils;

class Receiver implements MessageUpcall {

	static final Logger log = LoggerFactory.getLogger(Receiver.class);

	Factory<Chain> chainFactory = new Factory<Chain>(Chain.class);
	Factory<WritableContainer<Tuple>> bufferFactory;

	Context context;
	Container<Chain> chainsToProcess;
	Buckets buckets;
	NetworkLayer net;
	StatisticsCollector stats;
	int myId;

	public Receiver(Context context,
			Factory<WritableContainer<Tuple>> bufferFactory) {
		this.context = context;
		this.chainsToProcess = context.getChainsToProcess();
		this.buckets = context.getTuplesBuckets();
		this.net = context.getNetworkLayer();
		this.stats = context.getStatisticsCollector();
		this.bufferFactory = bufferFactory;
		myId = net.getMyPartition();

	}

	@Override
	public void upcall(ReadMessage message) throws IOException,
			ClassNotFoundException {

		long time = System.currentTimeMillis();

		byte messageId = message.readByte();

		if (log.isDebugEnabled()) {
			Thread.currentThread().setName(
					"Upcall " + message.origin().ibisIdentifier() + ", id = "
							+ messageId);
			log.debug("Starting upcall");
		}

		switch (messageId) {
		case 0: // Chain to process
			Chain chain = chainFactory.get();
			chain.readFrom(new ReadMessageWrapper(message));
			endMessage(message, time, chain.getSubmissionId());

			if (log.isDebugEnabled()) {
				log.debug("Adding chain: "
						+ chain.toString(context.getActionsProvider(),
								context.getDataProvider(), -1));
			}
			try {
				chainsToProcess.add(chain);
			} catch (Exception e) {
			}
			chainFactory.release(chain);
			break;
		case 1: // Receive signal that remote node has some tuples to send to
			// local bucket or that a chain didn't have.
			int submissionNode = message.readInt();
			int idSubmission = message.readInt();
			int idBucket = message.readInt();
			long idChain = message.readLong();
			long idParentChain = message.readLong();
			int children = message.readInt();
			int replicatedFactor = message.readInt();
			boolean isResponsible = message.readBoolean();
			boolean isSorted = message.readBoolean();
			String cf = null;
			Object[] cfParams = null;
			if (isSorted) {
				cf = message.readString();
				int lengthSortingParams = message.readInt();
				if (lengthSortingParams > 0) {
					cfParams = new Object[lengthSortingParams];
					Utils.readObjectsFromMessage(message, cfParams);
				}
			}
			long bufferKey = message.readLong();
			Bucket bucket = buckets.getOrCreateBucket(submissionNode,
					idSubmission, idBucket, cf, cfParams);
			bucket.updateCounters(idChain, idParentChain, children,
					replicatedFactor, isResponsible);

			if (bufferKey != -1) {
			        finishMessage(message, time, idSubmission);
				int idRemoteNode = net.getPeerId(message.origin()
						.ibisIdentifier());
				net.signalsBucketToFetch(idSubmission, idBucket, idRemoteNode,
						bufferKey);
			} else {
			        endMessage(message, time, idSubmission);
				bucket.updateCounters(0, true);
			}

			break;
		case 2: // Signal chain is terminated
			idSubmission = message.readInt();
			idChain = message.readLong();
			idParentChain = message.readLong();
			replicatedFactor = message.readInt();
			children = message.readInt();
			finishMessage(message, time, idSubmission);
			context.getSubmissionsRegistry().updateCounters(idSubmission,
					idChain, idParentChain, children, replicatedFactor);
			break;
		case 3: // Termination
			context.getNetworkLayer().ibis.end();
			System.exit(0);
			break;
		case 4: // Request to transfer a bucket
			long bucketKey = message.readLong();
			int submissionId = message.readInt();
			int bucketId = message.readInt();
			long ticket = message.readLong();
			int sequence = message.readInt();
			int nrequest = message.readInt();
			endMessage(message, time, submissionId);
			net.addRequestToSendTuples(bucketKey,
					net.getPeerId(message.origin().ibisIdentifier()),
					submissionId, bucketId, ticket, sequence, nrequest);
			break;
		case 5: // A bucket to copy to local
			ticket = message.readLong();
			submissionId = message.readInt();
			bucketId = message.readInt();
			sequence = message.readInt();
			bucketKey = message.readLong();
			nrequest = message.readInt();
			isSorted = message.readBoolean();
			String cFunction = null;
			Object[] cParams = null;
			if (isSorted) {
				cFunction = message.readString();
				int lengthSortingParams = message.readInt();
				if (lengthSortingParams > 0) {
					cParams = new Object[lengthSortingParams];
					Utils.readObjectsFromMessage(message, cParams);
				}
			}
			boolean data = message.readBoolean();

			if (data) {
				net.removeActiveRequest(ticket);
				WritableContainer<Tuple> container = bufferFactory.get();
				container.readFrom(new ReadMessageWrapper(message));
				boolean isFinished = message.readBoolean();
				
				bucket = buckets.getOrCreateBucket(myId, submissionId,
						bucketId, cFunction, cParams);

				try {
					bucket.addAll(container, cFunction != null, bufferFactory);
				} catch (Exception e) {
					log.error("Failed in adding the elements", e);
				}

				bucket.updateCounters(sequence, isFinished);
				if (!isFinished) {
				    finishMessage(message, time, submissionId);
					// Need to request for another fetch
					int remoteNodeId = net.getPeerId(message.origin()
							.ibisIdentifier());
					net.signalsBucketToFetch(submissionId, bucketId,
							remoteNodeId, bucketKey, ++sequence, ++nrequest);
				} else {
				    endMessage(message, time, submissionId);
				}

			} else {
				finishMessage(message, time, submissionId);
				int remoteNodeId = net.getPeerId(message.origin()
						.ibisIdentifier());
				// Resubmit it
				net.signalsBucketToFetch(submissionId, bucketId, remoteNodeId,
						bucketKey, sequence, ++nrequest);
			}
			break;
		case 6: // Statistics Collector
			while (message.readByte() == 1) {
				submissionId = message.readInt();
				int nParams = message.readInt();
				while (nParams-- > 0) {
					int lengthString = message.readInt();
					byte[] list = new byte[lengthString];
					message.readArray(list);
					long value = message.readLong();
					stats.addCounter(myId, submissionId, new String(list),
							value);
				}
			}
			/*
			 * No need to finish message. Upcall returns. message.finish(); if
			 * (log.isDebugEnabled()) { log.debug("Finished upcall, t = " +
			 * (System.currentTimeMillis() - time)); }
			 */
			break;
		case 7:
			// New job to submit
			JobDescriptor job = new JobDescriptor();
			job.readFrom(new ReadMessageWrapper(message));
			message.finish();
			net.startMonitorCounters();
			net.broadcastStartMonitoring();
			if (log.isDebugEnabled()) {
				log.debug("Got job: " + job.toString(context.getDataProvider()));
			}

			try {
				Submission submission = null;
				submission = context.getSubmissionsRegistry()
						.waitForCompletion(context, job);

				// Read the bucket
				bucket = buckets.getExistingBucket(
						submission.getSubmissionId(),
						submission.getAssignedBucket());
				// Not needed: already done when waitForCompletion returns.
				// bucket.waitUntilFinished();

				net.stopMonitorCounters();
				net.broadcastStopMonitoring();
				WritableContainer<Tuple> tmpBuffer = bufferFactory.get();
				tmpBuffer.clear();
				boolean isFinished = bucket.removeChunk(tmpBuffer);
				// Write a reply to the origin containing the results of this
				// submission
				WriteMessage reply = net.getMessageToSend(message.origin()
						.ibisIdentifier(), NetworkLayer.queryReceiverPort);
				reply.writeDouble(submission.getExecutionTimeInMs());

				tmpBuffer.writeTo(new WriteMessageWrapper(reply));
				reply.writeBoolean(isFinished);
				if (!isFinished) {
					reply.writeLong(bucket.getKey());
				}
				reply.finish();

				if (log.isDebugEnabled()) {
					log.debug("Sent reply");
				}

				context.getSubmissionsRegistry().getStatistics(job, submission);

				// bufferFactory.release(tmpBuffer);
				tmpBuffer = null;
				if (isFinished) {
					buckets.clearSubmission(submission.getSubmissionId());
					Runtime.getRuntime().gc();
				}
				context.getSubmissionsRegistry().releaseSubmission(submission);
			} catch (Exception e) {
				log.error("Error", e);
			}
			break;
		case 8:
			// Statistics request
			submissionId = message.readInt();
			message.finish();
			stats.sendStatisticsAway();
			if (myId != 0) {
				// Node 0 has the answer, so cannot clear yet.
				buckets.clearSubmission(submissionId);
				context.getSubmissionCache().clearAll(submissionId);
				Runtime.getRuntime().gc();
			} else {
				// No GC yet, because that intervenes with the connection setup
				// to deliver the answer.
				context.getSubmissionCache().clearAll(submissionId);
			}
			break;

		case 9: // Return results from a large buffer
			bucketKey = message.readLong();
			message.finish();

			bucket = buckets.getExistingBucket(bucketKey);
			WritableContainer<Tuple> tmpBuffer = bufferFactory.get();
			tmpBuffer.clear();
			boolean isFinished = bucket.removeChunk(tmpBuffer);

			WriteMessage reply = net.getMessageToSend(message.origin()
					.ibisIdentifier(), NetworkLayer.queryReceiverPort);
			tmpBuffer.writeTo(new WriteMessageWrapper(reply));
			reply.writeBoolean(isFinished);
			reply.finish();

			if (log.isDebugEnabled()) {
				log.debug("Sent reply isFinished=" + isFinished + " tmpBuffer="
						+ tmpBuffer.getNElements());
			}

			// bufferFactory.release(tmpBuffer);
			tmpBuffer = null;
			if (isFinished) {
				buckets.clearSubmission((int) (bucketKey >> 32));
				Runtime.getRuntime().gc();
			}
			break;
		case 10: // Broadcast object
			sequence = message.readInt();
			submissionId = message.readInt();
			Object[] keys = (Object[]) message.readObject();
			Object[] values = (Object[]) message.readObject();
			message.finish();

			// Put the obj in the cache
			for (int i = 0; i < keys.length; i++) {
				context.getSubmissionCache().putObjectInCache(submissionId,
						keys[i], values[i]);
			}

			// Return value
			WriteMessage msg = net.getMessageToSend(message.origin()
					.ibisIdentifier());
			msg.writeByte((byte) 11);
			msg.writeInt(sequence);
			msg.finish();
			break;
		case 11: // Acknowledge object is broadcasted
			sequence = message.readInt();
			message.finish();

			NetworkLayer.CountInfo remaining;
			synchronized (net.activeBroadcasts) {
				remaining = net.activeBroadcasts.get(sequence);
			}
			synchronized (remaining) {
				remaining.count--;
				if (remaining.count == 0) {
					remaining.notify();
				}
			}
			break;
		case 12: // Request to send object
			sequence = message.readInt();
			submissionId = message.readInt();
			keys = (Object[]) message.readObject();
			message.finish();

			// Read the objects from the cache
			values = new Object[keys.length];
			for (int i = 0; i < keys.length; i++) {
				values[i] = context.getSubmissionCache().getObjectFromCache(
						submissionId, keys[i]);
				if (values[i] == null) {
					values[i] = SerializableNull.instance;
				}
			}

			// Return value
			msg = net.getMessageToSend(message.origin().ibisIdentifier(),
					NetworkLayer.nameBcstReceiverPort);
			msg.writeByte((byte) 13);
			msg.writeInt(sequence);
			msg.writeObject(values);
			msg.finish();

			break;
		case 13: // Receive objects
			sequence = message.readInt();
			values = (Object[]) message.readObject();
			message.finish();

			NetworkLayer.CountInfo r;
			synchronized (net.activeRetrievals) {
				r = net.activeRetrievals.get(sequence);
			}

			// Remove the null values
			for (int i = 0; i < values.length; ++i) {
				if (values[i] instanceof SerializableNull)
					values[i] = null;
			}

			synchronized (r) {
				r.count--;
				r.receivedObjects.add(values);
				if (r.count == 0) {
					r.notify();
				}
			}
			break;
		case 14: // Request to execute custom code on every node
			sequence = message.readInt();
			int nodeId = message.readInt();
			submissionId = message.readInt();
			String code = message.readString();
			message.finish();

			boolean response = true;
			try {
				@SuppressWarnings("unchecked")
				Class<? extends RemoteCodeExecutor> clazz = (Class<? extends RemoteCodeExecutor>) Class
						.forName(code);
				RemoteCodeExecutor rm = clazz.newInstance();
				ActionContext ac = new ActionContext(context,
						context.getDataProvider(), nodeId, submissionId);
				rm.execute(ac);
			} catch (Exception e) {
				log.error("Failed in running the code", e);
				response = false;
			}

			// Return value
			msg = net.getMessageToSend(message.origin().ibisIdentifier());
			msg.writeByte((byte) 15);
			msg.writeInt(sequence);
			msg.writeBoolean(response);
			msg.finish();
			break;
		case 15: // Acknowledgment of the execution
			sequence = message.readInt();
			response = message.readBoolean();
			message.finish();

			// Update the counter
			NetworkLayer.CountInfo counter;
			synchronized (net.activeCodeExecutions) {
				counter = net.activeCodeExecutions.get(sequence);
			}

			synchronized (counter) {
				counter.count--;
				counter.success &= response;
				if (counter.count == 0) {
					counter.notify();
				}
			}

			break;
		case 16:
		    	net.startMonitorCounters();
		    	break;
		case 17:
		    	net.stopMonitorCounters();
		    	break;
		}

		if (log.isDebugEnabled()) {
			log.debug("Ended upcall, t = "
					+ (System.currentTimeMillis() - time));
		}
	}

	public void finishMessage(ReadMessage msg, long startTime, int submissionId)
			throws IOException {
		long bytes = msg.finish();
		long time = System.currentTimeMillis() - startTime;
		stats.addCounter(0, submissionId, "Time reading", time);
		stats.addCounter(0, submissionId, "Bytes read", bytes);
		// stats.addCounter(0, submissionId, "Messages read", 1);
	}
	

        public void endMessage(ReadMessage msg, long startTime, int submissionId)
                        throws IOException {
                long bytes = msg.bytesRead();
                long time = System.currentTimeMillis() - startTime;
                stats.addCounter(0, submissionId, "Time reading", time);
                stats.addCounter(0, submissionId, "Bytes read", bytes);
                // stats.addCounter(0, submissionId, "Messages read", 1);
        }
}