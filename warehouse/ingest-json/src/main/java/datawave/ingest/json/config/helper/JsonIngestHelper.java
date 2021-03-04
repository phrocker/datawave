package datawave.ingest.json.config.helper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.data.normalizer.SimpleGroupFieldNameParser;
import datawave.ingest.json.util.JsonObjectFlattener;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utilized by EventMapper to produce all the key/value pairs from each raw record, i.e, {@link RawRecordContainer}
 */
public class JsonIngestHelper extends ContentBaseIngestHelper {
    private static final MarkingFunctions markingFunctions = MarkingFunctionsFactory.createMarkingFunctions();
    
    protected JsonDataTypeHelper helper = null;
    protected JsonObjectFlattener flattener = null;
    protected SimpleGroupFieldNameParser groupNormalizer = new SimpleGroupFieldNameParser(true);
    
    @Override
    public void setup(Configuration config) {
        super.setup(config);
        helper = new JsonDataTypeHelper();
        helper.setup(config);
        this.setEmbeddedHelper(helper);
        flattener = helper.newFlattener();
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        
        if (null == flattener) {
            throw new IllegalStateException("JsonObjectFlattener was not initialized. Method 'setup' must be invoked first");
        }
        
        HashMultimap<String,Pair<String,Map<String,String>>> fields = HashMultimap.create();
        String jsonString = new String(event.getRawData());
        
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(jsonString);
        flattener.flatten(jsonElement.getAsJsonObject(), fields);
        
        return normalizeMap(getGroupNormalizedMap(fields));
    }
    
    protected Multimap<String,NormalizedContentInterface> getGroupNormalizedMap(HashMultimap<String,Pair<String,Map<String,String>>> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        fields.forEach((key, pair) -> {
            if (pair != null && pair.getLeft() != null) {
                Map<String,String> fieldMarkings = pair.getRight();
                if (fieldMarkings != null) {
                    Map<String,String> defaultMarkings = markingsHelper.getFieldMarking(key);
                    if (defaultMarkings == null)
                        defaultMarkings = markingsHelper.getDefaultMarkings();
                    try {
                        ColumnVisibility defaultVis = markingFunctions.translateToColumnVisibility(defaultMarkings);
                        ColumnVisibility fieldVis = markingFunctions.translateToColumnVisibility(fieldMarkings);
                        ColumnVisibility combined = new ColumnVisibility(new ColumnVisibility(Stream.of(defaultVis, fieldVis).map(ColumnVisibility::flatten)
                                        .filter(b -> b.length > 0).map(b -> "(" + new String(b, UTF_8) + ")").collect(Collectors.joining("|")).getBytes(UTF_8))
                                        .flatten());
                        fieldMarkings = markingFunctions.translateFromColumnVisibility(combined);
                    } catch (MarkingFunctions.Exception e) {
                        throw new IllegalArgumentException("Unable to combine markings: " + e.getMessage(), e);
                    }
                }
                results.put(key, new NormalizedFieldAndValue(key, pair.getLeft(), fieldMarkings));
            }
        });
        return groupNormalizer.extractFieldNameComponents(results);
    }
}
