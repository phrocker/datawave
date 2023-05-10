package datawave.query.tables.document.batch;

import datawave.query.DocumentSerialization;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

@Data
@RequiredArgsConstructor
@Builder
public class DocumentScanConfiguration{
    private final ClientContext context;
    private final TableId tableId;
    private final DocumentSerialization.ReturnType returnType;
    private final boolean docRawFields;
    private final int maxTabletThreshold;
    private final int maxTabletsPerThread;
    private final Authorizations authorizations;
    private final int numThreads;
    private final ExecutorService queryThreadPool;
    private final ScannerOptions options;
    private final ArrayList<Range> ranges;
    private final long timeout;
    private final int queueCapacity;

}
