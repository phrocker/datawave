package datawave.query.function.deserializer;

import java.io.InputStream;
import java.io.Serializable;
import java.util.TreeMap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentPayload;
import datawave.query.function.KryoCVAwareSerializableSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import datawave.query.function.serdes.DocumentPayloadKryo;

/**
 * Transform Kryo-serialized bytes back into a Document. Ordering of Attributes is <b>not</b> guaranteed across serialization.
 *
 * 
 *
 */
public class KryoDocumentDeserializer extends DocumentDeserializer implements Serializable {
    private static final long serialVersionUID = 1L;
    
    final transient Kryo kryo = new Kryo();
    
    public KryoDocumentDeserializer() {
        kryo.addDefaultSerializer(Attribute.class, new KryoCVAwareSerializableSerializer(true));
    }
    
    @Override
    public Document deserialize(InputStream data) {
        Input input = new Input(data);

//        Document document2 = kryo.readObject(input, Document.class);

        Input input2 = new Input(data);
        Document document = new Document();
        DocumentPayload documentPayload = new DocumentPayload(document);

        DocumentPayloadKryo.Deserializer documentPayloadKryoDeserializer =
                new DocumentPayloadKryo.Deserializer(kryo, documentPayload);
        documentPayloadKryoDeserializer.accept(input2);

         document._getDictionary().putAll(documentPayload.getDictionary());
        document.setTimestamp(documentPayload.getShardTimestamp());



        if (null == document) {
            throw new RuntimeException("Deserialized null Document");
        }

        input.close();
        
        return document;
    }
    
}
