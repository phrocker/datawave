package datawave.webservice.query.service;

import datawave.webservice.query.service.config.IndexingConfiguration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import java.util.Optional;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class ServiceConfiguration {


     public static ServiceConfiguration instance = new ServiceConfiguration();
     static{
          instance.setIndexingConfiguration(IndexingConfiguration.builder().build());
     }

     @Builder.Default
     IndexingConfiguration indexingConfiguration = IndexingConfiguration.builder().build();

     public static ServiceConfiguration getDefaultInstance(){
          return instance;
     }

}
