package io.github.pulsarkeeper.broker.checker;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.policies.data.ClusterData;

@Slf4j
public class ClusterChecker {

    // allowed characters for property, namespace, cluster and topic names are
    // alphanumeric (a-zA-Z_0-9) and these special chars -=:.
    // % is allowed as part of valid URL encoding
    public static final Pattern NAMED_ENTITY_PATTERN = Pattern.compile("^[-=:.\\w]*$");

    public static boolean checkClusterName(String clusterName) {
        Matcher m = NAMED_ENTITY_PATTERN.matcher(clusterName);
        return m.matches();
    }

    public static boolean checkClusterData(ClusterData clusterData) {
        String brokerServiceUrl = clusterData.getBrokerServiceUrl();
        String brokerServiceUrlTls = clusterData.getBrokerServiceUrlTls();
        String serviceUrl = clusterData.getServiceUrl();
        String serviceUrlTls = clusterData.getServiceUrlTls();
        try {
            if (StringUtils.isNoneEmpty(brokerServiceUrl)) {
                URI uri = URI.create(brokerServiceUrl);
                return (uri.getScheme().equals("pulsar"));
            }
            if (StringUtils.isNoneEmpty(brokerServiceUrlTls)) {
                URI uri = URI.create(brokerServiceUrlTls);
                return (uri.getScheme().equals("pulsar+ssl"));
            }
            if (StringUtils.isNoneEmpty(serviceUrl)) {
                URI uri = URI.create(serviceUrl);
                return (uri.getScheme().equals("http"));
            }
            if (StringUtils.isNoneEmpty(serviceUrlTls)) {
                URI uri = URI.create(serviceUrlTls);
                return (uri.getScheme().equals("https"));
            }
        } catch (Throwable ex) {
            log.error("Got exception when check cluster data.");
        }
        return false;
    }
}
