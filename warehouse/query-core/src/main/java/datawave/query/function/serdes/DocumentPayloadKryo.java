package datawave.query.function.serdes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.DocumentPayload;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

public class DocumentPayloadKryo {

    protected Kryo kryo;

    protected DocumentPayloadKryo(Kryo kryo) {
        this.kryo = kryo;
    }

    public static class Serializer extends DocumentPayloadKryo implements Consumer<DocumentPayload> {

        protected Output output;
        protected boolean reducedSize;

        public Serializer(Kryo kryo, Output output) {
            super(kryo);
            this.output = output;
        }

        public void accept(DocumentPayload documentPayload) {
//            kryo.writeReferenceOrNull(output, documentPayload, false);
//            output.writeByte(1);
            output.writeInt(documentPayload.getCount(), true);
            output.writeBoolean(documentPayload.isTrackSizes());
            output.writeLong(documentPayload.getSize(), true);

            output.writeInt(documentPayload.getDictionary().size(), true);

            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : documentPayload.getDictionary().entrySet()) {
                // Write out the field name
                // writeAscii fails to be read correctly if the value has only one character
                // need to use writeString here
                output.writeString(entry.getKey());

                Attribute<?> attribute = entry.getValue();
                output.writeString(attribute.getClass().getName());
                attribute.write(kryo, output, reducedSize);
            }
            output.writeLong(documentPayload.getShardTimestamp());
        }
    }

    public static class Deserializer extends DocumentPayloadKryo implements Consumer<Input> {

        DocumentPayload documentPayload;
        TreeMap<String, Attribute<?>> dictionary;

        public Deserializer(Kryo kryo, DocumentPayload documentPayload) {
            super(kryo);
            this.documentPayload = documentPayload;
            this.dictionary = documentPayload.getDictionary();
        }

        public void accept(Input input) {

            int count = input.readInt(true);
            boolean trackSizes = input.readBoolean();
            long _bytes = input.readLong(true);

            int numAttrs = input.readInt(true);


            for (int i = 0; i < numAttrs; i++) {


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
                        Constructor<?> ctor = (Constructor<?>)clz.getDeclaredConstructor(new Class[0]);
                        ctor.setAccessible(true);
                        attr = (Attribute<?>) ctor.newInstance();
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
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
            long shardTimestamp = input.readLong();

        }
    }
}
