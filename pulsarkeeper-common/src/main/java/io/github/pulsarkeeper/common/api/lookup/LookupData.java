package io.github.pulsarkeeper.common.api.lookup;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class LookupData {
    private String serviceURL;
    private String serviceTlsURL;
    private String brokerServiceURL;
    private String brokerServiceTlsURL;
}
