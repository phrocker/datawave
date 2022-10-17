package datawave.query.function.serializer;

import com.google.common.collect.Maps;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class JsonMetadataSerializer extends DocumentSerializer{

    public JsonMetadataSerializer(boolean reducedResponse, boolean allowCompression) {
        super(reducedResponse, allowCompression);
    }


    @Override
    public Map.Entry<Key, Value> apply(Map.Entry<Key, Document> from) {
//        try (TraceScope s = Trace.startSpan("Document Serialization")) {
//            if (s.getSpan() != null) {
//                s.getSpan().addKVAnnotation("Serialization type", this.concreteName);
//            }

        byte[] bytes = serialize(from.getValue());

//            if (s.getSpan() != null) {
//                s.getSpan().addKVAnnotation("Raw size", Integer.toString(bytes.length));
//            }

//            Value v = getValue(bytes, s);
        Value v = getValue(from.getKey(),bytes);

        return Maps.immutableEntry(from.getKey(), v);
//        }
    }

    private byte[] computeIdentifier(Key key) {
        ByteSequence row = key.getRowData();
        ByteSequence cf = key.getColumnFamilyData();

        // only append the last 2 tokens (the datatype and uid)
        // we are expecting that they may be prefixed with a count (see sortedUIDs in the DefaultQueryPlanner / QueryIterator)
        int nullCount = 0;
        int index = -1;
        for (int i = 0; i < cf.length() && nullCount < 2; i++) {
            if (cf.byteAt(i) == 0) {
                nullCount++;
                if (index == -1) {
                    index = i;
                }
            }
        }
        int dataTypeOffset = index + 1;
        int offset = cf.offset() + dataTypeOffset;
        int length = cf.length() - dataTypeOffset;

        byte[] bytes = new byte[row.length()-4 + length + 1];
        System.arraycopy(row.getBackingArray(), row.offset()+4, bytes, 0, row.length()-4);
        System.arraycopy(cf.getBackingArray(), offset, bytes, row.length() + 1, length-row.length());
        return bytes;
    }

    public static int getDataLength(byte [] doc){
        ByteBuffer buf = ByteBuffer.wrap(doc,3,doc.length-3);
        return buf.getInt();
    }

    public static byte[] getIdentifier(byte[] doc, int dataLength) {
        if (doc.length <= 7 || dataLength == 0){
            // if we don't have a document then we will return an empty identifier
            return new byte[0];
        }
        ByteBuffer buf = ByteBuffer.wrap(doc,7+dataLength,doc.length - dataLength - 7);
        byte [] array = new byte [ doc.length - dataLength - 7 ];
        buf.get(array, 0, doc.length - dataLength - 7);
        return array;
    }


    protected Value getValue(Key key, byte[] document) {
        byte[] header;
        byte[] identifier;
        byte[] dataToWrite;

        // Only compress the data if it's greater than minCompressionSize in size (bytes)
        if (DocumentSerialization.NONE != this.compression && document.length > minCompressionSize) {
            header = DocumentSerialization.getHeader(compression);
            dataToWrite = DocumentSerialization.writeBody(document, this.compression);
//            if (span.getSpan() != null) {
//                span.getSpan().addKVAnnotation("Compressed size", Integer.toString(dataToWrite.length));
//            }
        } else {
            header = DocumentSerialization.getHeader();
            dataToWrite = document;
        }
        identifier = computeIdentifier(key);

        ByteBuffer buf = ByteBuffer.allocate(identifier.length + 4 + header.length + dataToWrite.length);
        buf.put(header);
        // writes 4 bytes
        buf.putInt(dataToWrite.length);
        buf.put(dataToWrite);
        buf.put(identifier);
        return new Value(buf.array());
    }
}
