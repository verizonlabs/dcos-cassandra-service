package com.mesosphere.dcos.cassandra.common.tasks;

import org.apache.mesos.scheduler.plan.Completable;
import org.apache.mesos.scheduler.plan.Phase;

import java.util.List;

/**
 * Interface for managers of ClusterTask execution (e.g Backup, Restore, Cleanup, ... )
 *
 * @param <Context> the {@link ClusterTaskContext} used by the implementing manager
 */
public interface ClusterTaskManager<R extends ClusterTaskRequest> extends Completable {

    static boolean canStart(ClusterTaskManager<?> manager) {
        return !manager.isInProgress();
    }
    static boolean canStop(ClusterTaskManager<?> manager) {
        return manager.isInProgress();
    }

    void start(R request);
    void stop();
    boolean isInProgress();
    boolean isComplete();
    List<Phase> getPhases();
}
