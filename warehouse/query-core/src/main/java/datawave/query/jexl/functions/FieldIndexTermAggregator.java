package datawave.query.jexl.functions;

import datawave.marking.ColumnVisibilityCache;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.tld.TLD;
import datawave.query.util.Tuple2;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Aggregator for TF keys. TF keys that will be aggregated will be matching row and dataType/uid. FIELD/VALUE are not evaluated for performance reasons since
 * the likelihood of a collision is extremely small
 */
public class FieldIndexTermAggregator extends TermFrequencyAggregator {

    public FieldIndexTermAggregator(Set<String> fieldsToKeep, EventDataQueryFilter attrFilter, int maxNextCount) {
        super(fieldsToKeep, attrFilter, maxNextCount);
    }

    public FieldIndexTermAggregator(Set<String> fieldsToKeep, EventDataQueryFilter attrFilter) {
        this(fieldsToKeep, attrFilter, -1);
    }
    
    @Override
    protected List<Tuple2<String,String>> parserFieldNameValue(Key topKey) {
        DatawaveKey parser = new DatawaveKey(topKey);
        return Arrays.asList(new Tuple2<>(parser.getFieldName(), parser.getFieldValue()));
    }
    
    @Override
    protected ByteSequence parseFieldNameValue(ByteSequence cf, ByteSequence cq) {
        // field index starts a field value. since that can have nulls within it we want to
        // use the
        ArrayList<Integer> cqNulls = TLD.instancesOf(0, cq, -1);
        final int fieldNameLength = cf.length() - 3; // length - len("fi\x00")
        final int startFv =0;
        int nullEnd = cqNulls.size()-1;
        if (nullEnd >= 3){
            nullEnd-=2; // remove the last two;
        }
        final int stopFv = cqNulls.get(nullEnd);
        byte[] fnFv = new byte[fieldNameLength + 1 + stopFv - startFv];

        System.arraycopy(cf.getBackingArray(), 3, fnFv, 0, fieldNameLength);
        System.arraycopy(cq.getBackingArray(), startFv + cq.offset(), fnFv, fieldNameLength + 1, stopFv - startFv);
        return new ArrayByteSequence(fnFv);
    }
    
    @Override
    protected ByteSequence parsePointer(ByteSequence qualifier) {
        ArrayList<Integer> nullLocations = TLD.instancesOf(0, qualifier, -1);
        final int start = nullLocations.get(0);
        return qualifier.subSequence(start+1,qualifier.length());
    }
    
    @Override
    protected boolean samePointer(Text row, ByteSequence pointer, Key key) {
        if (row.equals(key.getRow())) {
            ByteSequence pointer2 = parsePointer(key.getColumnQualifierData());
            return (pointer.equals(pointer2));
        }
        return false;
    }
    
    @Override
    protected Key getSeekStartKey(Key current, ByteSequence pointer) {
        // CQ = dataType\0UID\0Normalized field value\0Field name
        // seek to the next documents TF
        return new Key(current.getRow(), current.getColumnFamily(), new Text(pointer + Constants.NULL_BYTE_STRING + Constants.MAX_UNICODE_STRING));
    }
    

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        Key key = itr.getTopKey();
        Text row = key.getRow();
        ByteSequence pointer = parsePointer(getPointerData(key));
        Key nextKey = key;
        while (nextKey != null && samePointer(row, pointer, nextKey)) {
            Key topKey = nextKey;
            List<Tuple2<String,String>> fieldNameValues = parserFieldNameValue(topKey);

            for (Tuple2<String,String> fieldNameValue : fieldNameValues) {
                Attribute<?> attr = attrs.create(fieldNameValue.first(), fieldNameValue.second(), topKey, true);
                // only keep fields that are index only and pass the attribute filter
                boolean toKeep = (fieldsToKeep == null || fieldsToKeep.contains(JexlASTHelper.removeGroupingContext(fieldNameValue.first())))
                        && (filter == null || filter.keep(topKey));
                attr.setToKeep(toKeep);

                // Anything that is being kept has to be added to the doc to be returned, if we aren't keeping only add to the doc if necessary for evaluation
                if (toKeep || (filter == null || filter.apply(new AbstractMap.SimpleEntry<>(topKey, null)))) {
                    doc.put(fieldNameValue.first(), attr);
                }
            }
            itr.next();
            nextKey = (itr.hasTop() ? itr.getTopKey() : null);
        }

        Key docKey = new Key(row, new Text(pointer.toArray()), new Text(), ColumnVisibilityCache.get(key.getColumnVisibilityData()), key.getTimestamp());
        Attribute<?> attr = new DocumentKey(docKey, false);
        doc.put(Document.DOCKEY_FIELD_NAME, attr);

        if (doc.size() == 1 && doc.get(Document.DOCKEY_FIELD_NAME) != null) {
            key = null;

            // empty the document
            doc.remove(Document.DOCKEY_FIELD_NAME);
            return key;
        }
        return TLD.buildParentKey(row, pointer, parseFieldNameValue(docKey.getColumnFamilyData(), key.getColumnQualifierData()), key.getColumnVisibility(),
                key.getTimestamp());
    }
}
