package datawave.webservice.query.service.config;

public class IndexingConfiguration {

    // options
    boolean enableIndexInfoUidToDayIntersectionBypass = true;
    boolean enableRangeScannerLimitDays = false;

    //

    private static IndexingConfiguration instance = new IndexingConfiguration();
    
    public boolean isEnableIndexInfoUidToDayIntersectionBypass() {
        return enableIndexInfoUidToDayIntersectionBypass;
    }
    
    public void setEnableIndexInfoUidToDayIntersectionBypass(boolean enableIndexInfoUidToDayIntersectionBypass) {
        this.enableIndexInfoUidToDayIntersectionBypass = enableIndexInfoUidToDayIntersectionBypass;
    }

    public boolean isEnableRangeScannerLimitDays() {
        return enableRangeScannerLimitDays;
    }

    public void setEnableRangeScannerLimitDays(boolean enableRangeScannerLimitDays) {
        this.enableRangeScannerLimitDays = enableRangeScannerLimitDays;
    }
    
    public static IndexingConfiguration getDefaultInstance() {
        return instance;
    }
    
}
