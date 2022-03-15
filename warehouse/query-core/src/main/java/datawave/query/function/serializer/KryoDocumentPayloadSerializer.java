package datawave.query.function.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.DocumentPayload;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class KryoDocumentPayloadSerializer {

    private int count;
    private boolean trackSizes;
    private long size;
    private TreeMap<String, Attribute<?>> dictionary;
    private long shardTimestamp;

    public void visit(DocumentPayload documentPayload) {

    }

    public void read(Kryo kryo, Input input) {
        this.count = input.readInt(true);
        trackSizes = input.readBoolean();
        this.size = input.readLong(true);

        int numAttrs = input.readInt(true);

        this.dictionary = new TreeMap<>();

        for (int i = 0; i < numAttrs; i++) {
            // Get the fieldName
            String fieldName = input.readString();

            // Get the class name for the concrete Attribute
            String attrClassName = input.readString();
            Class<?> clz;

            // Get the Class for the name of the class of the concrete Attribute
            try {
                clz = Class.forName(attrClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            Attribute<?> attr;
            if (Attribute.class.isAssignableFrom(clz)) {
                // Get an instance of the concrete Attribute
                try {
                    attr = (Attribute<?>) clz.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

            } else {
                throw new ClassCastException("Found class that was not an instance of Attribute");
            }
            // Reload the attribute
            attr.read(kryo, input);

            // Add the attribute back to the Map
            this.dictionary.put(fieldName, attr);
        }

        this.shardTimestamp = input.readLong();

//        this.invalidateMetadata();
    }

    public void write(DataOutput out) throws IOException {
        write(out, false);
    }

    public void write(DataOutput out, boolean reducedResponse) throws IOException {
        WritableUtils.writeVInt(out, count);
        out.writeBoolean(trackSizes);
        WritableUtils.writeVLong(out, size);

        // Write out the number of Attributes we're going to store
        WritableUtils.writeVInt(out, this.dictionary.size());

        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : this.dictionary.entrySet()) {
            // Write out the field name
            WritableUtils.writeString(out, entry.getKey());

            // Write out the concrete Attribute class
            WritableUtils.writeString(out, entry.getValue().getClass().getName());

            // Defer to the concrete instance to write() itself
            entry.getValue().write(out);
        }

        WritableUtils.writeVLong(out, shardTimestamp);
    }

}
