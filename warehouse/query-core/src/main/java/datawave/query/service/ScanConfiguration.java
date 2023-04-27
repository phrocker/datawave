package datawave.query.service;

import datawave.query.config.DocumentQueryConfiguration;
import datawave.webservice.query.service.ServiceConfiguration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class ScanConfiguration {

    ServiceConfiguration queryConfig;

    String tableName;
    Set<Authorizations> auths;
    int threads;

    @Builder.Default
    boolean docRawFields=false;

    int queueCapacity;
    int maxTabletsPerRequest;
    int maxTabletThreshold;

/*    public static ScanConfiguration from(DocumentQueryConfiguration config){
        var builder = ScanConfiguration.builder().auths(config.getAuthorizations()).docRawFields(config.getDocRawFields()).
    }

 */
}
