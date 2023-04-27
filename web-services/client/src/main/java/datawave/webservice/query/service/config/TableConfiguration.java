package datawave.webservice.query.service.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableConfiguration {
    String dataTable;
    String forwardIndexTable;
    String reverseIndexTable;

}
