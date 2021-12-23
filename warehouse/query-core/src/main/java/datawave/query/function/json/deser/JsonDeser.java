package datawave.query.function.json.deser;

import com.google.gson.*;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;

public class JsonDeser implements com.google.gson.JsonSerializer<Document>,com.google.gson.JsonDeserializer<Document>{

    private static void addJsonObject(Attribute<?> arrayAttr,String name, JsonArray jsonDocument){
        if (arrayAttr instanceof TypeAttribute){
            if (((TypeAttribute)arrayAttr).getType() instanceof NumberType){
                JsonObject obj = new JsonObject();
                obj.addProperty(name,(BigDecimal)((TypeAttribute)arrayAttr).getType().denormalize());
                jsonDocument.add(obj);
            }
            else{
                JsonObject obj = new JsonObject();
                obj.addProperty(name,arrayAttr.toString());
                jsonDocument.add(obj);
            }
        }
    }

    private static void addJsonObject(Attribute<?> attr,String name, JsonObject jsonDocument){
        if (attr instanceof Attributes){
            // we have an array
            JsonArray array = new JsonArray();
            for(Attribute<?> subA : ((Attributes)attr).getAttributes()){
                addJsonObject(subA,name,array);
            }
            jsonDocument.add(name,array);
        }
        if (attr instanceof TypeAttribute){
            if (((TypeAttribute)attr).getType() instanceof NumberType){
                jsonDocument.addProperty(name,(BigDecimal)((TypeAttribute)attr).getType().denormalize());
            }
            else{
                jsonDocument.addProperty(name,attr.toString());
            }
        }
    }


    public JsonElement serialize(Document document, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject jsonDocument = new JsonObject();

        for(Map.Entry<String, Attribute<?>> entry : document.getDictionary().entrySet()){
            Attribute<?> attr = entry.getValue();
            addJsonObject(attr,entry.getKey(),jsonDocument);
        }



        return jsonDocument;
    }

    private static void populateAttribute(JsonElement element,String name, Document doc){
        Key key = new Key();
        if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()){
            NumberType type = new NumberType(element.getAsString());
            TypeAttribute<?> attr = new TypeAttribute<>(type,key,true);
            doc.put(name,attr);
        }
        else{
            NoOpType type = new NoOpType(element.getAsString());
            TypeAttribute<?> attr = new TypeAttribute<>(type,key,true);
            doc.put(name,attr);
        }
    }

    private static void populateAttributes(JsonArray array,String name, Document doc){

    }
    @Override
    public Document deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        final Document doc = new Document();

        if (jsonElement instanceof JsonObject){
            ((JsonObject)jsonElement).entrySet().stream().forEach(
                    x->{ // Entry<String,JsonElement>
                        if (x.getValue() instanceof JsonArray){
                            // we have Attributes
                            populateAttributes((JsonArray)x.getValue(),x.getKey(),doc);
                        }
                        else{
                            populateAttribute(x.getValue(),x.getKey(),doc);
                        }
                    }
            );
        }

        return doc;
    }
}