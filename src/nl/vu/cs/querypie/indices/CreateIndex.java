package nl.vu.cs.querypie.indices;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.Arch;
import arch.actions.InputSplitter;
import arch.actions.SendTo;
import arch.actions.WriteToFile;
import arch.data.types.TBoolean;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.files.FilesLayer;
import arch.storage.TupleComparator;
import arch.submissions.JobDescriptor;
import arch.submissions.Submission;
import arch.utils.Configuration;
import arch.utils.Consts;

public class CreateIndex {

    static final Logger log = LoggerFactory.getLogger(CreateIndex.class);

    boolean startIbis = false;

    public void run(String[] args) throws Exception {

	Configuration conf = new Configuration();

	for (int i = 0; i < args.length; ++i) {
	    if (args[i].equals("-i")) {
		startIbis = true;
	    }

	    if (args[i].equals("--n-proc-threads")) {
		conf.setInt(Consts.N_PROC_THREADS, Integer.valueOf(args[++i]));
	    }

	    if (args[i].equals("--n-res-threads")) {
		conf.setInt(Consts.N_RES_THREADS, Integer.valueOf(args[++i]));
	    }
	}

	conf.setBoolean(Consts.START_IBIS, startIbis);
	conf.set(Consts.STORAGE_IMPL, FilesLayer.class.getName());
	conf.set(FilesLayer.IMPL_FILE_READER, CompressedReader.class.getName());
	conf.set(WriteToFile.FILE_IMPL_WRITER, CompressedWriter.class.getName());
	conf.setBoolean(Consts.STATS_ENABLED, true);

	Arch arch = new Arch();
	arch.startup(conf);

	if (arch.isServer()) {
	    JobDescriptor job = new JobDescriptor();

	    job.setPrintIntermediateStats(true);
	    job.excludeExecutionMainChain(true);
	    job.setInputTuple(new Tuple(new TString(args[0]), new TString(
		    CompressedFilesFilter.class.getName())));
	    job.setAvailableRules(InputSplitter.class.getName());
	    job.addAction(POSCreator.class);
	    job.addAction(POSHashPartitioner.class);
	    job.addAction(SendTo.class, new Tuple(new TString(SendTo.MULTIPLE),
		    new TBoolean(true), new TInt(0), new TBoolean(false),
		    new TString(TupleComparator.class.getName()), new TInt(0),
		    new TInt(1), new TInt(2)));
	    job.addAction(WriteToFile.class, new Tuple(new TString(args[1])));

	    log.info("Submit job ...");

	    Submission submission = arch.waitForCompletion(job);

	    arch.shutdown();
	    System.out.println("Execution time: "
		    + submission.getExecutionTimeInMs());
	    System.exit(0);
	}
    }

    public static void main(String[] args) throws Exception {

	// Parse eventual command line options
	if (args.length < 1) {
	    System.out
		    .println("Usage: CreateIndex <input dir> <output dir> -i (start ibis)");
	    return;
	}

	new CreateIndex().run(args);
    }
}
