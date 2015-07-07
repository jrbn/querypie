package nl.vu.cs.querypie;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.storage.disk.JavaGATFilesInterface;
import nl.vu.cs.querypie.storage.disk.PlainTripleFile;
import nl.vu.cs.querypie.storage.disk.RDFStorage;

import org.gridlab.gat.GAT;
import org.gridlab.gat.GATContext;
import org.gridlab.gat.Preferences;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryPIE {

	static final Logger log = LoggerFactory.getLogger(QueryPIE.class);

	// public static boolean ENABLE_COMPLETENESS = true;
	public static final int PARALLEL_JOIN_THREADS = 8;
	public static final boolean COUNT_LOOKUPS = true;

	private boolean useGAT = false;

	private void parseArgs(String[] args, Configuration conf) {

		conf.set("input.schemaDir", args[0] + "/closure");
		conf.set("input.dirIndexes", args[0] + "/index");

		for (int i = 1; i < args.length; ++i) {

			if (args[i].equals("--cacheURLs")) {
				conf.set("input.cacheURLs", args[++i]);
			}

			if (args[i].equals("--generatePoolName")) {
				conf.setBoolean("rewritePoolName", true);
			}

			if (args[i].equals("--with-ibis-server")) {
				conf.setBoolean(Consts.START_IBIS, true);
			}

			if (args[i].equals("--n-proc-threads")) {
				conf.setInt(Consts.N_PROC_THREADS, Integer.valueOf(args[++i]));
			}

			if (args[i].equals("--subject-threshold")) {
				conf.setInt("subject.threshold", Integer.valueOf(args[++i]));
			}

			if (args[i].equals("--cache-location")) {
				conf.set(RDFStorage.CACHE_LOCATION, args[++i]);
			}

			if (args[i].equals("--caching-iterator")) {
				conf.set(RDFStorage.ITERATOR_CLASS,
						"reasoner.storagelayer.CachingPatternIterator");
			}

			if (args[i].equals("--local-cache-location")) {
				conf.set(RDFStorage.LOCAL_CACHE_LOCATION, args[++i]);
			}

			if (args[i].equals("--clean-cache")) {
				conf.setBoolean(RDFStorage.CLEAN_CACHE, true);
			}

			if (args[i].equals("--javagat")) {
				useGAT = true;
			}

			if (args[i].equals("--no-multithread-startup")) {
				conf.setBoolean("startup.multithread", false);
			}

			if (args[i].equals("--dict")) {
				conf.set(RDFStorage.DICT_DIR, args[++i]);
			}
		}
	}

	public void startReasoner(String[] args) throws Exception {

		Ajira arch = new Ajira(true);

		Configuration conf = arch.getConfiguration();
		parseArgs(args, conf);
		conf.set("indexFileImpl", PlainTripleFile.class.getName());
		InputLayer.setDefaultInputLayerClass(RDFStorage.class, conf);

		if (useGAT) {
			log.debug("Going to use JavaGAT...");
			GATContext context = new GATContext();
			Preferences prefs = new Preferences();
			prefs.put("file.adaptor.name", "local,commandlinessh");
			prefs.put("fileinputstream.adaptor.name", "local,copying");
			context.addPreferences(prefs);
			GAT.setDefaultGATContext(context);

			conf.set(RDFStorage.FILES_INTERFACE,
					JavaGATFilesInterface.class.getName());
		}

		conf.setBoolean(Consts.SEND_TUPLES_COMPRESSED, false); // Probably not
																// worth the
																// effort
		arch.startup();
	}

	public static void main(String[] args) {

		// Parse eventual command line options
		if (args.length < 1) {
			System.out.println("Usage: HReason <data> <options>");
			return;
		}

		try {
			new QueryPIE().startReasoner(args);
		} catch (Exception e) {
			log.error("Error in starting the cluster", e);
		}
	}
}
