package datawave.query.tables.document.batch;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.KryoCVAwareSerializableSerializer;
import datawave.query.function.json.deser.JsonDeser;
import datawave.query.tables.serialization.JsonDocument;
import datawave.query.tables.serialization.SerializedDocument;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Map;

public class DocumentKeyConversion {


    static final transient ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>();

    static final JsonDeser jsonDeser = new JsonDeser();
    static final JsonParser jsonParser = new JsonParser();

    public static SerializedDocumentIfc getDocument(DocumentSerialization.ReturnType returnType, boolean docRawFields, TKeyValue kv){
        SerializedDocumentIfc document = null;
        byte [] array = kv.value.array();
        int offset = 0;
        int size = array.length;
        if (  !org.apache.thrift.TBaseHelper.wrapsFullArray(kv.value)){
            size = kv.value.remaining();
            offset = kv.value.arrayOffset() + kv.value.position();
        }
        if (DocumentSerialization.ReturnType.kryo == returnType) {
            if (kryo.get() == null){
                kryo.set(new Kryo());
                kryo.get().addDefaultSerializer(Attribute.class, new KryoCVAwareSerializableSerializer(true));
            }

            Input input = new Input(DocumentSerialization.consumeHeader(array,offset,size));
            Document doc  = kryo.get().readObject(input, Document.class);

            if (null == doc) {
                throw new RuntimeException("Deserialized null Document");
            }

            input.close();
            document = new SerializedDocument(doc);

        } else if (DocumentSerialization.ReturnType.json == returnType) {
            InputStream jsonStream  = new ByteArrayInputStream(array, offset+3, size - 3);


            Reader rdr = new InputStreamReader(jsonStream);
            JsonObject jsonObject = jsonParser.parse(rdr).getAsJsonObject();
            document = new JsonDocument(jsonObject,kv.getKey(),size-3);


        }
        else if (DocumentSerialization.ReturnType.jsondocument == returnType) {
            InputStream jsonStream  = new ByteArrayInputStream(array, offset+3, size - 3);

            Reader rdr = new InputStreamReader(jsonStream);
            JsonObject jsonObject = jsonParser.parse(rdr).getAsJsonObject();
            Document doc = jsonDeser.deserialize(jsonObject, null, null);
            document = new SerializedDocument(doc);
        }
        else{
            throw new UnsupportedOperationException("only kryo and json are supported");
        }

        return document;

    }

    public static SerializedDocumentIfc getDocument(DocumentSerialization.ReturnType returnType, boolean docRawFields, Map.Entry<Key, Value> keyValue) {
        return getDocument(returnType,docRawFields,new TKeyValue(keyValue.getKey().toThrift(),ByteBuffer.wrap(keyValue.getValue().get())));
    }

}
