package nl.vu.cs.querypie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

import nl.vu.cs.ajira.AjiraClient;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.WriteToBucket;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.JobFailedException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.JobProperties;
import nl.vu.cs.querypie.reasoner.CalculateClosure;
import nl.vu.cs.querypie.reasoner.QSQBCAlgo;
import nl.vu.cs.querypie.reasoner.ReasoningUtils;
import nl.vu.cs.querypie.reasoner.RuleBCAlgo;
import nl.vu.cs.querypie.reasoning.expand.TreeExpander;
import nl.vu.cs.querypie.sparql.SPARQLParser;
import nl.vu.cs.querypie.sparql.SPARQLPrintOutput;
import nl.vu.cs.querypie.sparql.SPARQLQueryExecutor;
import nl.vu.cs.querypie.sparql.SPARQLQueryOptimizer;
import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	boolean qsq = false;

	BufferedReader in;
	PrintWriter out;

	private String cluster;

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

	public AjiraClient getQueryResults(String propertiesFile, String query,
			boolean excludeExplicit, boolean waitForStatistics, boolean sparql,
			boolean rules) {

		try {

			Job job = new Job();

			if (rules) {
				JobProperties properties = new JobProperties();
				properties.putProperty("availableRules",
						TreeExpander.class.getName());
				job.setProperties(properties);
			}

			ActionSequence actions = new ActionSequence();
			if (closure) {
				// Execute the closure
				ActionConf c = ActionFactory
						.getActionConf(QueryInputLayer.class);
				c.setParamString(QueryInputLayer.S_INPUTLAYER,
						DummyLayer.class.getName());
				c.setParamWritable(QueryInputLayer.W_QUERY,
						new nl.vu.cs.ajira.actions.support.Query());
				actions.add(c);

				CalculateClosure.applyTo(null,
						TupleFactory.newTuple(new TBoolean(incomplete)),
						actions);
			} else if (!sparql) {
				// Parse the input tuple
				String[] values = query.split(" ");

				RDFTerm v1 = new RDFTerm();
				RDFTerm v2 = new RDFTerm();
				RDFTerm v3 = new RDFTerm();
				v1.setValue(Long.valueOf(values[0]));
				v2.setValue(Long.valueOf(values[1]));
				v3.setValue(Long.valueOf(values[2]));

				if (rules) {
					if (!qsq) {
						RuleBCAlgo.applyTo(v1, v2, v3, !excludeExplicit,
								actions);
					} else {
						log.info("Using QSQ algorithm");
						QSQBCAlgo.applyTo(v1, v2, v3, actions);
					}
				} else {
					ReasoningUtils.getResultsQuery(actions,
							TupleFactory.newTuple(v1, v2, v3), true);
				}

				// Copy the results in a bucket
				ActionConf c = ActionFactory.getActionConf(WriteToBucket.class);
				c.setParamStringArray(WriteToBucket.SA_TUPLE_FIELDS,
						RDFTerm.class.getName(), RDFTerm.class.getName(),
						RDFTerm.class.getName());
				c.setParamBoolean(WriteToBucket.B_OUTPUT_BUCKET_ID, false);
				actions.add(c);

			} else {
				intermediateStats = false;
				nl.vu.cs.ajira.actions.support.Query q = new nl.vu.cs.ajira.actions.support.Query(
						new TString(query));
				ActionConf c = ActionFactory
						.getActionConf(QueryInputLayer.class);
				c.setParamString(QueryInputLayer.S_INPUTLAYER,
						DummyLayer.class.getName());
				c.setParamWritable(QueryInputLayer.W_QUERY, q);
				actions.add(c);

				actions.add(ActionFactory.getActionConf(SPARQLParser.class));

				c = ActionFactory.getActionConf(SPARQLQueryOptimizer.class);
				c.setParamInt(SPARQLQueryOptimizer.I_MAX_LEVELS, 2);
				actions.add(c);

				if (qsq) {
					log.info("Using QSQ algorithm");
				}
				c = ActionFactory.getActionConf(SPARQLQueryExecutor.class);
				c.setParamBoolean(SPARQLQueryExecutor.B_QSQ, qsq);
				actions.add(c);

				actions.add(ActionFactory
						.getActionConf(SPARQLPrintOutput.class));

				c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						TString.class.getName());
				actions.add(c);

				c = ActionFactory.getActionConf(WriteToBucket.class);
				c.setParamStringArray(WriteToBucket.SA_TUPLE_FIELDS,
						TString.class.getName());
				c.setParamBoolean(WriteToBucket.B_OUTPUT_BUCKET_ID, false);
				actions.add(c);
			}

			job.setPrintIntermediateStats(intermediateStats);
			job.setPrintStatistics(true);
			job.setActions(actions);

			return new AjiraClient(propertiesFile, job);
		} catch (Exception e) {
			log.error("Error", e);
		}

		return null;
	}

	public void query(String[] args) throws InterruptedException, IOException,
			JobFailedException {
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

			if (args[i].equals("-cluster")) {
				cluster = args[++i];
			}

			if (args[i].equals("--excludeExplicit")) {
				excludeExplicit = true;
			}

			if (args[i].equals("--qsq")) {
				qsq = true;
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

		if (!rules && excludeExplicit) {
			System.out
					.println("No rules is applied and explicit triples are excluded. The results can only be 0. Exiting...");
			return;
		}

		// Launch query
		AjiraClient client = null;
		if (!closure) {
			if (fileSparqlQuery == null) {
				if (inputPattern == null) {
					System.out.println("Type triple pattern: ");
					inputPattern = new BufferedReader(new InputStreamReader(
							System.in)).readLine();
				}
				System.out
						.println("Processing query " + inputPattern + " ... ");
				client = getQueryResults(cluster, inputPattern,
						excludeExplicit, true, false, rules);
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

				client = getQueryResults(cluster, sparqlQuery, excludeExplicit,
						true, true, rules);
			}
		} else {
			client = getQueryResults(cluster, null, false, true, true, rules);
		}
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		boolean finished = client.getResult(tuples);
		double time = client.getTime();
		System.out.println("Time execution (ms) = " + time);
		long ntuples = tuples.size();
		displayedResults = 0;
		processTuples(tuples);

		while (!finished) {
			// Get message and process more tuples
			tuples.clear();
			finished = client.getMoreResults(tuples);
			ntuples += tuples.size();
			processTuples(tuples);
		}

		System.out.println("\n");
		System.out.println("Triples = " + ntuples);
	}

	public void processTuples(ArrayList<Tuple> tuples) {
		SimpleData[] row = null;

		try {
			while (tuples.size() > 0 && displayedResults++ < nDisplayResults) {
				Tuple tuple = tuples.remove(tuples.size() - 1);
				if (row == null) {
					row = new SimpleData[tuple.getNElements()];
				}
				for (int i = 0; i < row.length; ++i) {
					row[i] = tuple.get(i);
				}

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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		new Query().query(args);
	}
}
