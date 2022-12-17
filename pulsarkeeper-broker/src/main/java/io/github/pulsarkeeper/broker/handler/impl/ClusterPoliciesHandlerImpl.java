package io.github.pulsarkeeper.broker.handler.impl;

import io.github.pulsarkeeper.broker.handler.ClusterPoliciesHandler;
import io.github.pulsarkeeper.broker.service.ClusterService;
import io.github.pulsarkeeper.broker.service.impl.ClusterServiceImpl;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
public class ClusterPoliciesHandlerImpl implements ClusterPoliciesHandler {
    private ClusterService clusterService;

    public ClusterPoliciesHandlerImpl(ClusterServiceImpl clusterService) {

    }
}
