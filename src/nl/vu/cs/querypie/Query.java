package nl.vu.cs.querypie;

import ibis.ipl.Ibis;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import nl.vu.cs.querypie.reasoner.RuleBCAlgo;
import nl.vu.cs.querypie.reasoner.RuleGetPattern;
import nl.vu.cs.querypie.reasoner.rules.executors.TreeExpander;
import nl.vu.cs.querypie.sparql.SPARQLExecutorHashJoin;
import nl.vu.cs.querypie.sparql.SPARQLParser;
import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.actions.PutIntoBucket;
import arch.actions.SendTo;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;
import arch.data.types.TBoolean;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.net.NetworkLayer;
import arch.net.ReadMessageWrapper;
import arch.net.WriteMessageWrapper;
import arch.storage.container.WritableContainer;
import arch.submissions.JobDescriptor;
import arch.utils.Consts;

public class Query {

    static final Logger log = LoggerFactory.getLogger(Query.class);

    boolean excludeExplicit = false;

    int nDisplayResults = 10;
    int displayedResults = 0;
    boolean dictionary = false;
    public boolean intermediateStats = false;
    public String dictionaryHost;
    Socket connection = null;
    String fileSparqlQuery = null;
    String inputPattern = null;
    String closurePath = null;
    boolean closure = false;
    boolean rules = false;
    boolean incomplete = false;

    BufferedReader in;
    PrintWriter out;

    DataProvider dp;

    public String[] getText(SimpleData[] row) {
	String[] answer = new String[row.length];
	try {

	    if (connection == null) {
		connection = new Socket(dictionaryHost, 4444);
		out = new PrintWriter(connection.getOutputStream(), true);
	    }

	    String request = "";
	    for (SimpleData value : row) {
		request += " " + value.toString();
	    }
	    out.println(request.trim());
	    in = new BufferedReader(new InputStreamReader(
		    connection.getInputStream()));
	    for (int i = 0; i < row.length; ++i) {
		answer[i] = in.readLine();
		if (answer[i].length() > 20) {
		    answer[i] = "..."
			    + answer[i].substring(answer[i].length() - 20,
				    answer[i].length());
		}
	    }
	} catch (Exception e) {
	}

	return answer;
    }

    Ibis ibis = null;
    SendPort port = null;

    private ReceivePort rp;

    public ReadMessageWrapper getQueryResults(String query,
	    boolean excludeExplicit, boolean waitForStatistics, boolean sparql,
	    boolean rules) {
	try {

	    JobDescriptor job = new JobDescriptor();
	    if (rules)
		job.setAvailableRules(TreeExpander.class.getName());
	    else
		job.setAvailableRules(null);
	    job.excludeExecutionMainChain(excludeExplicit);
	    job.setWaitForStatistics(waitForStatistics);

	    job.setAssignedOutputBucket(0); // Fetch the tuples from bucket 0

	    if (closure) {
		// Execute the closure
		job.addAction(
			nl.vu.cs.querypie.reasoner.CalculateClosure.class,
			new Tuple(new TBoolean(incomplete)));

		job.addAction(SendTo.class, new Tuple(new TString(SendTo.THIS),
			new TBoolean(false), new TInt(0)));

		job.setInputLayer(Consts.DUMMY_INPUT_LAYER_ID);
		job.setInputTuple(new Tuple());

	    } else if (!sparql) {
		// Parse the input tuple
		RDFTerm v1 = new RDFTerm();
		RDFTerm v2 = new RDFTerm();
		RDFTerm v3 = new RDFTerm();
		String[] values = query.split(" ");
		v1.setValue(Long.valueOf(values[0]));
		v2.setValue(Long.valueOf(values[1]));
		v3.setValue(Long.valueOf(values[2]));
		Tuple t = new Tuple(v1, v2, v3);

		job.setInputTuple(t);

		if (rules) {
		    if (!incomplete) {
			job.addAction(RuleBCAlgo.class, t);
		    } else {
			job.addAction(RuleGetPattern.class, new Tuple(
				new TBoolean(true)));
		    }
		}

		job.addAction(PutIntoBucket.class, new Tuple(new TInt(0)));
	    } else {
		job.setPrintIntermediateStats(false);
		job.addAction(SPARQLParser.class);
		job.addAction(SPARQLExecutorHashJoin.class);

		job.addAction(SendTo.class, new Tuple(new TString(SendTo.THIS),
			new TBoolean(false), new TInt(0)));

		job.setInputLayer(Consts.DUMMY_INPUT_LAYER_ID);
		job.setInputTuple(new Tuple(new TString(query)));
	    }

	    job.setPrintIntermediateStats(intermediateStats);

	    // Connect to the server
	    if (ibis == null) {
		ibis = IbisFactory.createIbis(NetworkLayer.ibisCapabilities,
			null, NetworkLayer.queryPortType,
			NetworkLayer.requestPortType,
			NetworkLayer.mgmtRequestPortType);
		IbisIdentifier server = ibis.registry().getElectionResult(
			"server");
		port = ibis.createSendPort(NetworkLayer.mgmtRequestPortType);
		port.connect(server, NetworkLayer.nameMgmtReceiverPort);
		rp = ibis.createReceivePort(NetworkLayer.queryPortType,
			NetworkLayer.queryReceiverPort);
		rp.enableConnections();
	    }

	    WriteMessage msg = port.newMessage();
	    msg.writeByte((byte) 7);
	    job.writeTo(new WriteMessageWrapper(msg));
	    msg.finish();

	    return new ReadMessageWrapper(rp.receive());

	} catch (Exception e) {
	    log.error("Error", e);
	}

	return null;
    }

    public void query(String[] args) throws InterruptedException, IOException {
	if (args.length == 1 && args[0].equals("--help")) {
	    System.out
		    .println("Usage: [-d dictionaryHost --excludeExplicit "
			    + "--nResults 50 --rules <File with rules> "
			    + "--incomplete --intermediateStats --sparql <File with sparql query> --pattern <pattern>]");
	    return;
	}

	for (int i = 0; i < args.length; ++i) {

	    if (args[i].equals("-d")) {
		dictionary = true;
		dictionaryHost = args[++i];
	    }

	    if (args[i].equals("--excludeExplicit")) {
		excludeExplicit = true;
	    }

	    if (args[i].equals("--pattern")) {
		inputPattern = args[++i] + " " + args[++i] + " " + args[++i];
	    }

	    if (args[i].equals("--incomplete")) {
		incomplete = true;
	    }

	    if (args[i].equals("--intermediateStats")) {
		intermediateStats = true;
	    }

	    if (args[i].equals("--nResults")) {
		nDisplayResults = Integer.valueOf(args[++i]);
	    }

	    if (args[i].equals("--rules")) {
		rules = true;
	    }

	    if (args[i].equals("--closure")) {
		closure = true;
	    }

	    if (args[i].equals("--closurePath")) {
		closurePath = args[++i];
	    }

	    if (args[i].equals("--sparql")) {
		fileSparqlQuery = args[++i];
	    }
	}

	new RDFTerm();
	dp = new DataProvider();

	// // Parse the rules to apply
	// File fileRules = null;
	// if (this.fileRules != null) {
	// fileRules = new File(this.fileRules);
	// }

	// Launch query
	ReadMessageWrapper is = null;
	if (!closure) {
	    if (fileSparqlQuery == null) {
		if (inputPattern == null) {
		    System.out.println("Type triple pattern: ");
		    inputPattern = new BufferedReader(new InputStreamReader(
			    System.in)).readLine();
		}
		System.out
			.println("Processing query " + inputPattern + " ... ");
		is = getQueryResults(inputPattern, excludeExplicit, true,
			false, rules);
	    } else {
		String sparqlQuery = "";
		File file = new File(fileSparqlQuery);
		if (file != null && file.exists()) {
		    BufferedReader reader = new BufferedReader(new FileReader(
			    file));
		    String line = null;
		    while ((line = reader.readLine()) != null) {
			sparqlQuery += "\n" + line;
		    }
		    reader.close();
		    log.info("Going to execute query:\n" + sparqlQuery);
		} else {
		    log.warn("SPARQL file " + file.getName()
			    + " does not exist and will be ignored.");
		}

		is = getQueryResults(sparqlQuery, excludeExplicit, true, true,
			rules);
	    }
	} else {
	    is = getQueryResults(null, false, true, true, rules);
	}

	double time = is.readDouble();
	System.out.println("Time execution (ms) = " + time);
	WritableContainer<Tuple> tuples = new WritableContainer<Tuple>(
		Consts.TUPLES_CONTAINER_BUFFER_SIZE);
	tuples.readFrom(is);
	long ntuples = tuples.getNElements();
	boolean finished = is.readBoolean();
	long key = -1;
	if (!finished) {
	    key = is.readLong();
	}
	displayedResults = 0;
	processTuples(dp, tuples);
	is.closeMessage();

	while (!finished) {
	    // Get message and process more tuples
	    is = getMoreTuples(key);
	    tuples.readFrom(is);
	    ntuples += tuples.getNElements();
	    processTuples(dp, tuples);
	    finished = is.readBoolean();
	    is.closeMessage();
	}

	System.out.println("\n");
	System.out.println("Triples = " + ntuples);
	closeIbis();
    }

    public ReadMessageWrapper getMoreTuples(long key) {
	try {
	    WriteMessage msg = port.newMessage();
	    msg.writeByte((byte) 9);
	    msg.writeLong(key);
	    msg.finish();

	    return new ReadMessageWrapper(rp.receive());

	} catch (Exception e) {
	    log.error("Error", e);
	}
	return null;
    }

    public void processTuples(DataProvider dp, WritableContainer<Tuple> tuples) {
	Tuple tuple = new Tuple();
	SimpleData[] row = null;

	try {
	    while (tuples.remove(tuple) && displayedResults++ < nDisplayResults) {
		if (row == null) {
		    row = new SimpleData[tuple.getNElements()];
		    for (int i = 0; i < row.length; ++i) {
			row[i] = dp.get(tuple.getType(i));
		    }
		}

		tuple.get(row);

		if (dictionary) {
		    String[] values = getText(row);
		    String line = "";
		    for (int i = 0; i < row.length; ++i) {
			line += values[i] + "(" + row[i].toString() + ") ";
		    }
		    System.out.println(line.trim());
		} else {
		    String line = "";
		    for (SimpleData el : row) {
			line += el.toString() + " ";
		    }
		    line = line.trim();
		    System.out.println(line);
		}
	    }

	    if (row != null) {
		for (SimpleData el : row) {
		    dp.release(el);
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void closeIbis() {
	try {
	    ibis.end();
	} catch (Throwable e) {
	    log.debug(
		    "Ibis.end() gave exeception, but this is not fatal for a query",
		    e);
	}
    }

    public static void main(String[] args) throws Exception {
	new Query().query(args);
    }
}
