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
            output.writeInt(documentPayload.getCount(), true);
            output.writeBoolean(documentPayload.isTrackSizes());
            output.writeLong(documentPayload.getSize(), true);

            output.writeInt(documentPayload.getDictionary().size(), true);

            AttributeKryo.Serializer serializer =
                    new AttributeKryo.Serializer(kryo, output);

            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : documentPayload.getDictionary().entrySet()) {

                serializer.accept(entry);

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


            AttributeKryo.Deserializer deser = new AttributeKryo.Deserializer(kryo, documentPayload);
            for (int i = 0; i < numAttrs; i++) {

                deser.accept(input);

            }
            long shardTimestamp = input.readLong();

        }
    }
}
