package datawave.webservice.query.service.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexingConfiguration {


     public static IndexingConfiguration instance = new IndexingConfiguration();

     @Builder.Default
     boolean enableIndexInfoUidToDayIntersectionBypass=true;

     public static IndexingConfiguration getDefaultInstance(){
          return instance;
     }

}
