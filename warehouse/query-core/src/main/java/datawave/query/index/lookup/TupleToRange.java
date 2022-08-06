package datawave.query.index.lookup;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.ranges.RangeFactory;
import datawave.query.util.Tuple2;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Transforms information from the index into ranges used to search the shard table.
 *
 */
public class TupleToRange implements Function<Tuple2<String,IndexInfo>,Iterator<QueryPlan>> {
    
    private static final Logger log = Logger.getLogger(TupleToRange.class);
    protected JexlNode currentScript;
    protected JexlNode tree = null;
    protected ShardQueryConfiguration config;

    RangeBloomFilters bloom;

    /**
     * @param currentNode
     *            the jexl node
     * @param config
     *            a configuration
     */
    public TupleToRange(JexlNode currentNode, ShardQueryConfiguration config) {
        this.currentScript = currentNode;
        this.config = config;
        bloom = new RangeBloomFilters();

    }
    
    /**
     * Transform the index information into a QueryPlan by building ranges.
     *
     * @param tuple
     *            the tuple
     * @return a query plan iterator
     */
    public Iterator<QueryPlan> apply(Tuple2<String,IndexInfo> tuple) {
        String shard = tuple.first();
        IndexInfo indexInfo = tuple.second();
        
        JexlNode queryNode = currentScript;
        if (log.isTraceEnabled() && indexInfo.getNode() != null) {
            log.trace("Got it from tuple " + JexlStringBuildingVisitor.buildQuery(indexInfo.getNode()));
        }
        System.out.println(Thread.currentThread().getId() + " " + "Got it from tuple " + tuple.first() );
        if (isDocumentRange(indexInfo)) {
            
            return createDocumentRanges(queryNode, shard, indexInfo, config.isTldQuery(), bloom);
            
        } else if (isShardRange(shard)) {
            
            return createShardRange(queryNode, shard, indexInfo, bloom);
            
        } else {
            
            return createDayRange(queryNode, shard, indexInfo, bloom);
        }
    }
    
    /**
     * Building document ranges is only possible if the IndexInfo object contains document ids.
     *
     * @param indexInfo
     *            - object built from matches in the index.
     * @return - true if we can build document range(s).
     */
    public static boolean isDocumentRange(IndexInfo indexInfo) {
        return !indexInfo.uids().isEmpty();
    }
    
    /**
     *
     * @param shard
     *            a shard string
     * @return - true if the shard string is a shard range
     */
    public static boolean isShardRange(String shard) {
        return shard.lastIndexOf('_') > 0;
    }


    public static Iterator<QueryPlan> createDocumentRanges(JexlNode queryNode, String shard, IndexInfo indexMatches, boolean isTldQuery) {
        return createDocumentRanges(queryNode,shard,indexMatches,isTldQuery,null);
    }

    /**
     *
     *
     * @param queryNode
     *            a query node
     * @param shard
     *            shard string
     * @param indexMatches
     *            the index to pull uids
     * @param isTldQuery
     *            check for tld query
     * @return an iterator of query plans
     */
    public static Iterator<QueryPlan> createDocumentRanges(JexlNode queryNode, String shard, IndexInfo indexMatches, boolean isTldQuery, RangeBloomFilters bloom) {
        List<QueryPlan> ranges = Lists.newArrayListWithCapacity(indexMatches.uids().size());
        
        for (IndexMatch indexMatch : indexMatches.uids()) {
            
            String docId = indexMatch.getUid();
            Range range;
            if (isTldQuery) {
                range = RangeFactory.createTldDocumentSpecificRange(shard, docId);
            } else {
                range = RangeFactory.createDocumentSpecificRange(shard, docId);
            }
            
            if (log.isTraceEnabled())
                log.trace(queryNode + " " + indexMatch.getNode());
            
            // don't really want log statement if uid.getNode is null
            
            // Log info if indexMatch is not null
            if (log.isTraceEnabled() && null != indexMatch.getNode()) {
                
                // query node can be null in this case
                log.trace("Building " + range + " from " + (null == queryNode ? "NoQueryNode" : JexlStringBuildingVisitor.buildQuery(queryNode)) + " actually "
                                + JexlStringBuildingVisitor.buildQuery(indexMatch.getNode()));
            }
            System.out.println(Thread.currentThread().getId() + " " +"Creating ddoc "  + range);
            if (null != bloom) {
                if (!bloom.hasSeenDocOrShard(range)){
                    System.out.println("Adding range since it does not contain " + range);
                    ranges.add(new QueryPlan(indexMatch.getNode(), range));
                }
                else{
                    System.out.println("Not adding range since it might contain " + range);
                }
            }
            else {
                System.out.println("Not adding range since no bloom " + range);
                ranges.add(new QueryPlan(indexMatch.getNode(), range));
            }
        }
        return ranges.iterator();
    }

    public static Iterator<QueryPlan> createShardRange(JexlNode queryNode, String shard, IndexInfo indexInfo) {
        return createShardRange(queryNode,shard,indexInfo,null);
    }
    
    public static Iterator<QueryPlan> createShardRange(JexlNode queryNode, String shard, IndexInfo indexInfo, RangeBloomFilters bloom) {
        JexlNode myNode = queryNode;
        if (indexInfo.getNode() != null) {
            myNode = indexInfo.getNode();
        }
        
        Range range = RangeFactory.createShardRange(shard);
        
        if (log.isTraceEnabled() && null != myNode) {
            log.trace("Building shard " + range + " From " + JexlStringBuildingVisitor.buildQuery(myNode));
        }
        if (null != bloom) {
            System.out.println(Thread.currentThread().getId() + " " + "Creating shard " + range);
            if (!bloom.hasSeenShard(range))
            {
                return Collections.singleton(new QueryPlan(myNode, range)).iterator();
            }
            else{
                System.out.println("Not adding range since it might contain " + range);
            }
            return Collections.emptyIterator();
        }
        else{
            System.out.println("Not adding range since no bloom " + range);
            return Collections.singleton(new QueryPlan(myNode, range)).iterator();
        }
    }

    public static Iterator<QueryPlan> createDayRange(JexlNode queryNode, String shard, IndexInfo indexInfo) {
        return createDayRange(queryNode,shard,indexInfo,null);
    }
    
    public static Iterator<QueryPlan> createDayRange(JexlNode queryNode, String shard, IndexInfo indexInfo, RangeBloomFilters bloom) {
        JexlNode myNode = queryNode;
        if (indexInfo.getNode() != null) {
            myNode = indexInfo.getNode();
        }
        
        Range range = RangeFactory.createDayRange(shard);
        if (log.isTraceEnabled())
            log.trace("Building day" + range + " from " + (null == myNode ? "NoQueryNode" : JexlStringBuildingVisitor.buildQuery(myNode)));
        if (null != bloom) {
            System.out.println(Thread.currentThread().getId() + " " + "Creating day " + range);
            if (!bloom.hasSeenShard(range))
            {
                return Collections.singleton(new QueryPlan(myNode, range)).iterator();
            }
            else{
                System.out.println("Not adding range since it might contain " + range);
            }
            return Collections.emptyIterator();
        }
        else{
            System.out.println("Not adding range since no bloom " + range);
            return Collections.singleton(new QueryPlan(myNode, range)).iterator();
        }
    }

    private static class RangeBloomFilters {
        static final int D0C_EXPECTED_DEFAULT = 50000;
        static final int SHARD_EXPECTED_DEFAULT = 5000;
        static final double BLOOM_FPP_DEFAULT = 1e-15;
        private BloomFilter<Range> doc_bloom = null;
        private BloomFilter<Range> shard_bloom = null;

        public RangeBloomFilters(){
            this.doc_bloom = BloomFilter.create(new RangeFunnel(), D0C_EXPECTED_DEFAULT, BLOOM_FPP_DEFAULT);
            this.shard_bloom = BloomFilter.create(new RangeFunnel(), SHARD_EXPECTED_DEFAULT, BLOOM_FPP_DEFAULT);
        }

        public boolean hasSeenDocOrShard(Range docRange){
            if (doc_bloom.mightContain(docRange) || shard_bloom.mightContain(docRange)){
                return true;
            }
            doc_bloom.put(docRange);
            return false;
        }

        public boolean hasSeenShard(Range docRange){
            if (shard_bloom.mightContain(docRange)){
                return true;
            }
            System.out.println("Add to shard bloom " + docRange);
            shard_bloom.put(docRange);
            return false;
        }

    }

    public static class RangeFunnel implements Funnel<Range>, Serializable {

        private static final long serialVersionUID = -2126172579955897986L;

        @Override
        public void funnel(Range from, PrimitiveSink into) {
            into.putBytes(WritableUtils.toByteArray(from));
        }

    }

    public static class ShardFunnel implements Funnel<Range>, Serializable {

        private static final long serialVersionUID = -2126172579955897986L;

        @Override
        public void funnel(Range from, PrimitiveSink into) {
            into.putBytes(from.getStartKey().getRow().getBytes());
        }

    }
}
