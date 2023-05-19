package datawave.webservice.query.logic.deser;

import com.google.common.base.Joiner;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

public class ResultsPageAdapter extends TypeAdapter<JsonResultsPage> {

    @Override
    public void write(JsonWriter jsonWriter, JsonResultsPage resultsPage) throws IOException {
        // outside object
        jsonWriter.beginObject();

            // size
            jsonWriter.beginObject();
            jsonWriter.name("page_number");
            jsonWriter.value(resultsPage.getPageNumber());
            // size end
            jsonWriter.endObject();

            // size
            jsonWriter.beginObject();
            jsonWriter.name("size");
            jsonWriter.value(resultsPage.getPage().getResults().size());
            // size end
            jsonWriter.endObject();

            // events
            jsonWriter.beginObject();
            jsonWriter.name("events");
            jsonWriter.beginArray();
            jsonWriter.jsonValue(Joiner.on(",").join(resultsPage.getPage().getResults()));
            jsonWriter.endArray();
            // events end
            jsonWriter.endObject();

        // outside object
        jsonWriter.endObject();

    }

    @Override
    public JsonResultsPage read(JsonReader jsonReader) throws IOException {
        return null;
    }
}
