package io.github.pulsarkeeper.broker.resources;

import com.google.common.base.Joiner;

public class ResourcesHelpers {

    public static final String BASE_POLICIES_PATH = "/admin/policies";
    public static final String BASE_CLUSTERS_PATH = "/admin/clusters";
    public static final String FAILURE_DOMAIN = "failureDomain";

    public static String joinPath(String... parts) {
        StringBuilder sb = new StringBuilder();
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }
}
