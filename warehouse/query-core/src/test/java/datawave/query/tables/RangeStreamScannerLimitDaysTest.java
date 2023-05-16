package datawave.query.tables;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.DataTypeFilter;
import datawave.query.index.lookup.EntryParser;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.ScannerStream;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.QueryScannerHelper;
import datawave.query.util.Tuple2;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.*;

/**
 * Integration test for the {@link RangeStreamScanner}
 */
public class RangeStreamScannerLimitDaysTest extends RangeStreamScannerTest{

    @Before
    public void augmentConfig(){
        config.getServiceConfiguration().getIndexingConfiguration().setEnableRangeScannerLimitDays(true);
    }
}
