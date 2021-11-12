package datawave.queryrunner;

import com.beust.jcommander.JCommander;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.query.logic.QueryLogic;

import java.util.HashMap;
import java.util.Map;

public class ShardQueryRunner extends BaseQueryRunner {
    
    MetadataHelperFactory mhf = new MetadataHelperFactory();
    DateIndexHelperFactory dif = new DateIndexHelperFactory();
    
    @Override
    protected QueryLogic createQueryLogic() {
        return new ShardQueryLogic();
    }
    
    /**
     * Initialize will load the properties file and save it. Instances that override the base query runner may choose to select a different query logic.
     *
     * @throws Exception
     */
    @Override
    public void initialize(String queryString) throws Exception {
        super.initialize(queryString);
        
        ((ShardQueryLogic) queryLogic).setMetadataHelperFactory(mhf);
        ((ShardQueryLogic) queryLogic).setDateIndexHelperFactory(dif);
        Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
        querySyntaxParsers.put("LUCENE", new LuceneToJexlQueryParser());
        ((ShardQueryLogic) queryLogic).setQuerySyntaxParsers(querySyntaxParsers);
        ((ShardQueryLogic) queryLogic).setConfig(qpf.getShardQueryConfiguration());
        ((ShardQueryLogic) queryLogic).initialize(qpf.getShardQueryConfiguration(), getConnector(), queryObj, getAuthorizations());
    }
    
    @Override
    public void setupQuery() throws Exception {
        queryLogic.setupQuery(qpf.getShardQueryConfiguration());
    }
    
    public static void main(String[] args) throws Exception {
        
        try (BaseQueryRunner queryRunner = new ShardQueryRunner()) {
            
            JCommander.newBuilder().addObject(queryRunner).build().parse(args);

            // configure the query runner.
            queryRunner.configureOptions();

            // run the queries
            queryRunner.runQueries();

        }
        
    }
}
