package com.mesosphere.dcos.cassandra.scheduler.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.mesos.state.StateStore;

import java.util.Optional;


public class RegisteredCheck extends HealthCheck {
    public static final String NAME = "registered";
    private final StateStore stateStore;

    @Inject
    public RegisteredCheck(final StateStore stateStore) {
        this.stateStore = stateStore;
    }

    protected Result check() {
        final Result unhealthyResult = Result.unhealthy("Framework is not yet registered");
        final Optional<Protos.FrameworkID> frameworkID = stateStore.fetchFrameworkId();
        if (frameworkID.isPresent()) {
            String id = frameworkID.get().getValue();
            if (!id.isEmpty()) {
                return Result.healthy("Framework registered with id = " + id);
            } else {
                return unhealthyResult;
            }
        } else {
            return unhealthyResult;
        }
    }
}
