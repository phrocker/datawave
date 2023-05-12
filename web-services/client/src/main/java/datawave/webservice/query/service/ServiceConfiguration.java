package datawave.webservice.query.service;

import datawave.webservice.query.service.config.IndexingConfiguration;

/**
 * Provides a centralized place to support configuration to code changes
 */
public class ServiceConfiguration {
    
    public static ServiceConfiguration instance = new ServiceConfiguration();
    static {
        instance.setIndexingConfiguration(IndexingConfiguration.getDefaultInstance());
    }
    
    IndexingConfiguration indexingConfiguration;
    
    public IndexingConfiguration getIndexingConfiguration() {
        return indexingConfiguration;
    }
    
    public void setIndexingConfiguration(IndexingConfiguration indexingConfiguration) {
        this.indexingConfiguration = indexingConfiguration;
    }
    
    public static ServiceConfiguration getDefaultInstance() {
        return instance;
    }
    
}
