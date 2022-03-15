package datawave.query.attributes;


import java.util.Map;
import java.util.TreeMap;

public class DocumentPayload {

    private int count;
    private boolean trackSizes;
    private long size;
    private TreeMap<String, Attribute<?>> dictionary;
    private long shardTimestamp;

    public DocumentPayload(Document document) {
        this.dictionary = new TreeMap<>(document._getDictionary());
        this.count = document.getCount();
        this.trackSizes = document.isTrackSizes();
        this.size = document.getBytes();
    }

    public TreeMap<String,Attribute<? extends Comparable<?>>> getDictionary() {
        return dictionary;
    }

    public int getCount() {
        return count;
    }

    public boolean isTrackSizes() {
        return trackSizes;
    }

    public long getSize() {
        return size;
    }

    public long getShardTimestamp() {
        return shardTimestamp;
    }
}
