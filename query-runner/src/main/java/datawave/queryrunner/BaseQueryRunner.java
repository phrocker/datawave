package datawave.queryrunner;

import com.beust.jcommander.JCommander;
import com.google.common.base.Splitter;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ShardQueryLogic;

import com.beust.jcommander.Parameter;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;

public class BaseQueryRunner implements AutoCloseable {
    
    @Parameter(names = "-instance", description = "Accumulo instance name where DW Query iterators are installed", required = true)
    private String instance;
    
    @Parameter(names = "-zookeepers", description = "Comma separated list of zookeepers associated with the Accumulo instance.", required = true)
    private String zookeepers;
    
    @Parameter(names = "-user", description = "Accumulo user.", required = true)
    private String accumuloUser;
    
    @Parameter(names = "-password", description = "Accumulo user's password.", required = true, password = true)
    private String accumuloPassword;
    
    @Parameter(names = "-queryType", description = "Query type (LUCENE, JEXL) Default is LUCENE.")
    private String queryType;
    
    @Parameter(names = "-auths", description = "Comma separated list of authorizations")
    private String auths;
    
    @Parameter(names = "-query", description = "Your query to run")
    private String query;
    
    @Parameter(names = "-beginDate", description = "Begin Date for your query")
    private Date beginDate;
    
    @Parameter(names = "-endDate", description = "End date for your query")
    private Date endDate;
    
    @Parameter(names = "-queryThreads", description = "Query Threads run on this client")
    private int queryThreads = -1;
    
    @Parameter(names = "-maxResults", description = "Maximum number of query results")
    private int maxResults = Integer.MAX_VALUE;
    
    @Parameter(names = "-outFile", description = "Output file for the query results")
    private String outFile;

    @Parameter(names = "-loadProperties", description = "Loads properties. If specified in conjunction with command line options this will serve as a base.")
    private String loadPropertiesFile;

    @Parameter(names = "-saveProperties", description = "Output file to save properties for this execution")
    private String savePropertiesFile;
    
    // @TODO should explore whether we want to run other query logics.

    QueryPropertiesFile qpf = new QueryPropertiesFile();
    ShardQueryLogic queryLogic = null;
    
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
    
    private Query createQueryObject() {
        QueryImpl query = new QueryImpl();
        // public void initialize(String userDN, List<String> dnList, String queryLogicName, QueryParameters qp, Map<String,List<String>>
        // optionalQueryParameters) {
        // @Todo explore the need to set the DNs.
        List<String> dnList = new ArrayList<>();
        // @Todo Might be useful to support parameters via input files.
        QueryParameters params = new QueryParametersImpl();
        Map<String,List<String>> optionalQueryParameters = new HashMap<>();
        query.setQuery(this.query);
        query.initialize("", dnList, "ShardQueryLogic", params, optionalQueryParameters);
        return query;
    }
    
    public void initialize() throws Exception {
        if (StringUtils.isNotBlank(loadPropertiesFile)){
            FileInputStream fin = new FileInputStream(loadPropertiesFile);
            qpf.load(fin);
            // load the query from the properties file. if a new one is specified
            // then we'll replace the one in the file.
            if (null != qpf.getProperty("query") && StringUtils.isBlank(query)) {
                query = qpf.getProperty("query");
            }
            // set the query regardless of whether the previous query was set in the property file
            // if the properties file is to be saved, this will support automation.
            if (StringUtils.isNotBlank(query)){
                qpf.setProperty("query",query);
            }
        }
        // initialize from our options.
        queryLogic = new ShardQueryLogic();
        // allow override of properties
        if (queryThreads > 0) {
            qpf.setProperty("queryThreads", queryThreads);
        }
        Query query = createQueryObject();

        // save the properties file.
        if (StringUtils.isNotBlank(savePropertiesFile)) {
            FileOutputStream fos = new FileOutputStream(savePropertiesFile);
            qpf.store(fos,"");
            fos.close();;
        }
        queryLogic.initialize(qpf.getShardQueryConfiguration(), getConnector(), query, getAuthorizations());
        
    }
    
    @Override
    public void close() throws Exception {
        if (null != queryLogic) {
            queryLogic.close();
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        BaseQueryRunner queryRunner = new BaseQueryRunner();
        
        JCommander.newBuilder().addObject(queryRunner).build().parse(args);
        
        queryRunner.initialize();
        
    }
    
}
