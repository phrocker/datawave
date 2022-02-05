package datawave.query.scheduler;

import datawave.query.attributes.Document;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.DocumentLogic;
import datawave.query.tables.DocumentScannerImpl;
import datawave.query.tables.MyScannerFactory;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.configuration.QueryData;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class DocumentScheduler extends Scheduler<Document> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(DocumentScheduler.class);

    protected final DocumentQueryConfiguration config;
    protected final ScannerFactory scannerFactory;
    protected final AtomicInteger count = new AtomicInteger(0);

    protected DocumentSchedulerIterator iterator = null;

    /**
     * Statistics used for validation.
     */
    protected int rangesSeen = 0;

    public DocumentScheduler(DocumentQueryConfiguration config, ScannerFactory scannerFactory) {
        this.config = config;
        this.scannerFactory = scannerFactory;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Document> iterator() {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }

        this.iterator = new DocumentSchedulerIterator(this.config, this.scannerFactory);

        return this.iterator;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (null != this.iterator) {
            this.iterator.close();
        }

        log.debug("Ran " + count.get() + " queries for a single user query");
    }

    /*
     * (non-Javadoc)
     *
     * @see Scheduler#createBatchScanner(ShardQueryConfiguration, datawave.query.tables.ScannerFactory, datawave.webservice.query.configuration.QueryData)
     */
    @Override
    public DocumentScannerImpl createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return DocumentLogic.createDocumentScanner(DocumentQueryConfiguration.class.cast(config), MyScannerFactory.class.cast(scannerFactory), qd, config.getReturnType());
    }

    public class DocumentSchedulerIterator implements Iterator<Document> {

        protected final ShardQueryConfiguration config;
        protected final ScannerFactory scannerFactory;

        protected Iterator<QueryData> queries = null;
        protected Document currentDocument = null;
        protected DocumentScannerImpl currentBS = null;
        protected Iterator<Document> currentIter = null;

        protected volatile boolean closed = false;

        public DocumentSchedulerIterator(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
            this.config = config;
            this.scannerFactory = scannerFactory;
            this.queries = config.getQueries();
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            
            if (null != this.currentDocument) {
                return true;
            } else if (null != this.currentBS && null != this.currentIter) {
                if (this.currentIter.hasNext()) {
                    this.currentDocument = this.currentIter.next();
                    
                    return hasNext();
                } else {
                    this.currentBS.close();
                }
            }
            
            QueryData newQueryData = null;
            while (true) {
                if (this.queries.hasNext()) {
                    // Keep track of how many QueryData's we make
                    QueryData qd = this.queries.next();
                    if (null != qd.getRanges())
                        rangesSeen += qd.getRanges().size();
                    count.incrementAndGet();
                    if (null == newQueryData)
                        newQueryData = new QueryData(qd);
                    else {
                        newQueryData.getRanges().addAll(qd.getRanges());
                    }
                    
                } else
                    break;
            }
            
            if (null != newQueryData) {
                
                try {
                    this.currentBS = createBatchScanner(this.config, this.scannerFactory, newQueryData);
                } catch (TableNotFoundException e) {
                    throw new RuntimeException(e);
                }
                
                this.currentIter = this.currentBS.getDocumentIterator();
                
                return hasNext();
            }
            
            return false;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        @Override
        public Document next() {
            if (closed) {
                return null;
            }
            
            if (hasNext()) {
                Document cur = this.currentDocument;
                this.currentDocument = null;
                return cur;
            }
            
            return null;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public void close() {
            if (!closed) {
                closed = true;
            }
            if (null != this.currentBS) {
                this.currentBS.close();
            }
        }
    }
    
    @Override
    public ScanSessionStats getSchedulerStats() {
        return null;
    }
    
    public int getRangesSeen() {
        return rangesSeen;
    }
    
    public int getQueryDataSeen() {
        return count.get();
    }
    
}
