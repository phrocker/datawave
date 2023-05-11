package datawave.webservice.query.service.config;

public class IndexingConfiguration {
    
    private static IndexingConfiguration instance = new IndexingConfiguration();
    
    public boolean isEnableIndexInfoUidToDayIntersectionBypass() {
        return enableIndexInfoUidToDayIntersectionBypass;
    }
    
    public void setEnableIndexInfoUidToDayIntersectionBypass(boolean enableIndexInfoUidToDayIntersectionBypass) {
        this.enableIndexInfoUidToDayIntersectionBypass = enableIndexInfoUidToDayIntersectionBypass;
    }
    
    boolean enableIndexInfoUidToDayIntersectionBypass = true;
    
    public static IndexingConfiguration getDefaultInstance() {
        return instance;
    }
    
}
