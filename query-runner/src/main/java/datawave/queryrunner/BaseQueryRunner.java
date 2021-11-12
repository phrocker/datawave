package datawave.queryrunner;

import com.beust.jcommander.Parameter;
import com.google.common.base.Splitter;
import datawave.marking.MarkingFunctions;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.parser.lucene.LuceneQueryParser;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.queryrunner.converters.DateConverter;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class BaseQueryRunner implements AutoCloseable {
    
    @Parameter(names = "-instance", description = "Accumulo instance name where DW Query iterators are installed", required = true)
    protected String instance;
    
    @Parameter(names = "-zookeepers", description = "Comma separated list of zookeepers associated with the Accumulo instance.", required = true)
    protected String zookeepers;
    
    @Parameter(names = "-user", description = "Accumulo user.", required = true)
    protected String accumuloUser;
    
    @Parameter(names = "-password", description = "Accumulo user's password.", required = true, password = true)
    protected String accumuloPassword;
    
    @Parameter(names = "-queryType", description = "Query type (LUCENE, JEXL) Default is LUCENE.")
    protected String queryType;
    
    @Parameter(names = "-auths", description = "Comma separated list of authorizations")
    protected String auths;
    
    @Parameter(names = "-query", description = "Your query to run")
    protected String query;
    
    @Parameter(names = "-begindate", description = "Begin Date for your query", converter = DateConverter.class)
    protected Instant beginDate = null;
    
    @Parameter(names = "-enddate", description = "End date for your query", converter = DateConverter.class)
    protected Instant endDate = null;
    
    @Parameter(names = "-queryThreads", description = "Query Threads run on this client")
    protected int queryThreads = -1;
    
    @Parameter(names = "-maxResults", description = "Maximum number of query results")
    protected int maxResults = Integer.MAX_VALUE;
    
    @Parameter(names = "-outFile", description = "Output file for the query results")
    protected String outFile;
    
    @Parameter(names = "-loadProperties", description = "Loads properties. If specified in conjunction with command line options this will serve as a base.")
    protected String loadPropertiesFile;
    
    @Parameter(names = "-saveProperties", description = "Output file to save properties for this execution")
    protected String savePropertiesFile;
    
    @Parameter(names = "-queryFile", description = "Query file if provided that overrides the query argument")
    protected String queryFile;
    
    @Parameter(names = "-enableTrace", description = "Enables Tracing on SQL")
    private Boolean enableTrace = false;
    
    @Parameter(names = "-warmups", description = "Number of warmup iterations")
    private int warmups = 0;
    
    @Parameter(names = "-lucene", description = "Enables Lucene query type")
    private Boolean luceneQueryType = false;
    
    @Parameter(names = "-produceTiming", description = "Time each query's setup and execution")
    private Boolean produceTiming = false;
    
    @Parameter(names = "-queryFileHasName", description = "Dictates the first part of the string is the query name")
    private Boolean queryFileHasName = false;
    
    protected QueryPropertiesFile qpf = new QueryPropertiesFile();
    
    protected QueryLogic queryLogic = null;
    
    protected Query queryObj;
    
    MarkingFunctions markingFunctions = new MarkingFunctions.Default();
    
    protected BaseQueryRunner() {}
    
    /**
     * Returns the connector associated with this ZkInstance.
     *
     * @return
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    protected Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        ZooKeeperInstance zkInstance = new ZooKeeperInstance(instance, zookeepers);
        return zkInstance.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
    }
    
    protected Set<Authorizations> getAuthorizations() {
        Set<Authorizations> retAuths = new HashSet<>();
        Splitter.on(",").split(auths).forEach(x -> retAuths.add(new Authorizations(x)));
        return retAuths;
    }
    
    private Query createQueryObject(final String queryString) {
        QueryImpl query = new QueryImpl();
        
        // optionalQueryParameters) {
        // @Todo explore the need to set the DNs.
        List<String> dnList = new ArrayList<>();
        QueryParameters params = new QueryParametersImpl();
        params.setAuths(auths);
        if (luceneQueryType) {
            query.addParameter(datawave.query.QueryParameters.QUERY_SYNTAX.toString(), "LUCENE");
            
        }
        if (null != beginDate) {
            params.setBeginDate(Date.from(beginDate));
        }
        if (null != endDate) {
            params.setEndDate(Date.from(endDate));
        }
        params.setQuery(queryString);
        Map<String,List<String>> optionalQueryParameters = new HashMap<>();
        
        query.initialize("", dnList, "ShardQueryLogic", params, optionalQueryParameters);
        return query;
    }
    
    /**
     * warmup is enabled in the configuration.
     * 
     * @return
     */
    public boolean warmupEnabled() {
        return warmups > 0;
    }
    
    public void configureOptions() throws IOException {
        if (enableTrace) {
            System.out.println("Enabling trace");
            BasicConfigurator.configure();
            ThreadConfigurableLogger.getLogger(ShardQueryLogic.class).setLevel(Level.TRACE);
            ThreadConfigurableLogger.getLogger(DefaultQueryPlanner.class).setLevel(Level.TRACE);
        }
        if (StringUtils.isNotBlank(loadPropertiesFile)) {
            final FileInputStream fin = new FileInputStream(loadPropertiesFile);
            qpf.load(fin);
            // load the query from the properties file. if a new one is specified
            // then we'll replace the one in the file.
            if (null != qpf.getProperty("query") && StringUtils.isBlank(query)) {
                query = qpf.getProperty("query");
            }
            // set the query regardless of whether the previous query was set in the property file
            // if the properties file is to be saved, this will support automation.
            if (StringUtils.isNotBlank(query)) {
                qpf.setProperty("query", query);
            }
        }
        qpf.setProperty("dateIndexTableName", "datawave.dateIndex");
        qpf.setProperty("metadataTableName", "datawave.metadata");
        qpf.setProperty("shardTableName", "datawave.shard");
        qpf.setProperty("indexTableName", "datawave.shardIndex");
        qpf.setProperty("reverseIndexTableName", "datawave.shardreverseIndex");
        qpf.setProperty("modelTableName", "datawave.metadata");
    }
    
    /**
     * Initialize will load the properties file and save it. Instances that override the base query runner may choose to select a different query logic.
     *
     * @throws Exception
     */
    public void initialize(String queryString) throws Exception {
        
        // initialize from our options.
        queryLogic = createQueryLogic();
        // allow override of properties
        if (queryThreads > 0) {
            qpf.setProperty("queryThreads", queryThreads);
        }
        queryObj = createQueryObject(queryString);
        
        // save the properties file.
        if (StringUtils.isNotBlank(savePropertiesFile)) {
            final FileOutputStream fos = new FileOutputStream(savePropertiesFile);
            qpf.store(fos, "");
            fos.close();
        }
        qpf.getShardQueryConfiguration().setQuery(queryObj);
        qpf.getShardQueryConfiguration().setAccumuloPassword(accumuloPassword);
        
        // must set the markings function prior to initialization.
        queryLogic.setMarkingFunctions(markingFunctions);
        
    }
    
    public abstract void setupQuery() throws Exception;
    
    protected abstract QueryLogic createQueryLogic();
    
    @Override
    public void close() throws Exception {
        System.out.println("closing query");
        if (null != queryLogic) {
            queryLogic.close();
        }
    }
    
    public String runQuery(final String query, final Consumer<Map.Entry<Key,Value>> entryConsumer) throws Exception {
        
        initialize(query);
        
        Runnable task = () -> {
            try {
                setupQuery();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        
        task.run();
        
        if (null != queryLogic) {
            this.queryLogic.iterator().forEachRemaining(entryConsumer);
            queryLogic.close();
        }
        
        queryLogic = null;
        
        return query;
    }

    /**
     * Runs the query running timing if it is configured to do so.
     * @param query query to execute. query syntax type is configured globally
     * @param enableTiming enables timing for this execution.
     * @throws Exception
     */
    private void runQuery(String query, boolean enableTiming) throws Exception {
        TraceStopwatch tsw = new TraceStopwatch("Elapsed Query Timer");
        AtomicInteger results = new AtomicInteger();
        if (enableTiming) {
            tsw.start();
        }
        runQuery(query, kv -> {
            results.getAndIncrement();
        });
        if (enableTiming) {
            tsw.stop();
            System.out.println("Finished " + query + " in " + tsw.elapsed(TimeUnit.MILLISECONDS) + " milliseconds");
        }
    }
    
    private Pair<String,String> getQuery(final String query) {
        String queryName = "", queryString = "";
        if (queryFileHasName) {
            queryName = query.substring(0, query.indexOf(":"));
            queryString = query.substring(query.indexOf(":") + 1);
        } else {
            queryString = query;
        }
        return new ImmutablePair<>(queryName, queryString);
    }
    
    protected void runQueries() throws Exception {
        if (StringUtils.isBlank(queryFile)) {
            if (this.query.isEmpty()) {
                throw new IllegalArgumentException("You must supply a query or query file");
            }
            if (warmupEnabled()) {
                System.out.println("Warming up JVM...");
                IntStream.range(0, warmups).forEach(x -> {
                    try {
                        runQuery(this.query, false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                System.out.println("Warmup complete");
            }
            runQuery(this.query, true);
        } else {
            if (warmupEnabled()) {
                System.out.println("Warming up JVM...");
                try (final Stream<String> lines = Files.lines(Paths.get(queryFile), StandardCharsets.ISO_8859_1)) {
                    lines.limit(warmups).filter(x -> !x.isEmpty()).forEach(query1 -> {
                        try {
                            
                            runQuery(getQuery(query1).getValue(), true);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
                System.out.println("Warmup complete");
            }
            System.out.println("Running " + queryFile);
            try (Stream<String> stream = Files.lines(Paths.get(queryFile), StandardCharsets.ISO_8859_1)) {
                stream.filter(x -> !x.isEmpty()).forEach(query1 -> {
                    try {
                        System.out.println("Running " + getQuery(query1).getValue());
                        runQuery(getQuery(query1).getValue(), true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        
    }
}
