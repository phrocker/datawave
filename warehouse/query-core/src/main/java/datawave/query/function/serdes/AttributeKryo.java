package datawave.query.function.serdes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.DocumentPayload;

import java.util.TreeMap;
import java.util.function.Consumer;

public class AttributeKryo {

    protected Kryo kryo;

    protected AttributeKryo(Kryo kryo) {
        this.kryo = kryo;
    }
    public static class Serializer extends AttributeKryo implements Consumer<Attribute<?>> {

        protected Output output;
        public Serializer(Kryo kryo, Output output) {
            super(kryo);
            this.output = output;
        }

        public void accept(Attribute<?> attribute) {
            output.writeBoolean(attribute.isMetadataSet());
            if (attribute.isMetadataSet()) {
                byte[] cvBytes = attribute.getColumnVisibility().getExpression();
                output.writeInt(cvBytes.length, true);
                output.writeBytes(cvBytes);
                output.writeLong(attribute.getTimestamp());
            }
        }
    }

    public static class Deserializer extends AttributeKryo implements Consumer<Input> {

        DocumentPayload documentPayload;
        TreeMap<String, Attribute<?>> dictionary;

        public Deserializer(Kryo kryo, DocumentPayload documentPayload) {
            super(kryo);
            this.documentPayload = documentPayload;
            this.dictionary = documentPayload.getDictionary();
        }

        public void accept(Input input) {
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
            this.documentPayload.getDictionary().put(fieldName, attr);
        }

//        protected void readMetadata(Kryo kryo, Input input) {
//            if (input.readBoolean()) {
//                int size = input.readInt(true);
//
//                this.setMetadata(new ColumnVisibility(input.readBytes(size)), input.readLong());
//            } else {
//                this.clearMetadata();
//            }
//        }

    }






}
