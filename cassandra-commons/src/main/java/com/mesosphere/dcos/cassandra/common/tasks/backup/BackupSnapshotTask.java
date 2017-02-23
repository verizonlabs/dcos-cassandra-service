/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * BackupSnapshotTask extends CassandraTask to implement a task that
 * snapshots a set of key spaces and column families for a Cassandra cluster.
 * The task can only be launched successfully if the CassandraDaemonTask is
 * running on the targeted slave.
 * If the key spaces for the task are empty. All non-system key spaces are
 * backed up.
 * If the column families for the task are empty. All column families for the
 * indicated key spaces are backed up.
 */
public class BackupSnapshotTask extends CassandraTask {


    private static final Logger LOGGER = LoggerFactory.getLogger
            (BackupSnapshotTask.class);

    /**
     * The name prefix for BackupSnapshotTasks.
     */
    public static final String NAME_PREFIX = "snapshot-";

    /**
     * Gets the name of a BackupSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the BackupSnapshotTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a BackupSnapshotTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               taken.
     * @return The name of the BackupSnapshotTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }


    public static BackupSnapshotTask parse(final Protos.TaskInfo info) {
        return new BackupSnapshotTask(info);
    }


    public static BackupSnapshotTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final BackupRestoreContext context) {

        String name = nameForDaemon(daemon);
        CassandraData data = CassandraData.createBackupSnapshotData(
                "",
                context
                    .forNode(name)
                    .withLocalLocation(daemon.getVolumePath() + "/data"));

        Protos.TaskInfo.Builder completedTemplate = Protos.TaskInfo.newBuilder(template)
            .setName(name)
            .setTaskId(TaskUtils.toTaskId(name))
            .setData(data.getBytes());

        LOGGER.info("Executor command: {} ", completedTemplate.getExecutor().getCommand().toString());

        String command = completedTemplate.getExecutor().getCommand().getValue();
        Protos.ExecutorID execId = completedTemplate.getExecutor().getExecutorId();
        Protos.FrameworkID frameId = completedTemplate.getExecutor().getFrameworkId();
        List<Protos.Resource> resourceList = completedTemplate.getExecutor().getResourcesList();

        String[] split = command.split("volumename");
        String volumeName = split[1].split(" ")[0];

        completedTemplate.clearExecutor();

        Protos.ExecutorInfo.Builder newExec = Protos.ExecutorInfo.newBuilder()
                .setName(NAME_PREFIX + volumeName)
                .setExecutorId(execId)
                .setFrameworkId(frameId)
                .setContainer(Protos.ContainerInfo.newBuilder()
                        .setType(Protos.ContainerInfo.Type.MESOS)
                        .addVolumes(Protos.Volume.newBuilder()
                                .setHostPath("/var/lib/rexray/volumes/" + volumeName)
                                .setContainerPath("volume/data")
                                .setMode(Protos.Volume.Mode.RW)))
                .setCommand(Protos.CommandInfo.newBuilder().setValue("./executor/bin/cassandra-executor server executor/conf/executor.yml"));

        for (Protos.Resource resource: resourceList) {
            newExec.addResources(resource);
        }

        completedTemplate.setExecutor(newExec);

        Protos.TaskInfo finalTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate.build());
        return new BackupSnapshotTask(finalTemplate);
    }

    /**
     * Constructs a new BackupSnapshotTask.
     */
    protected BackupSnapshotTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public BackupSnapshotTask update(Protos.Offer offer) {
        return new BackupSnapshotTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public BackupSnapshotTask updateId() {
        return new BackupSnapshotTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public BackupSnapshotTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.BACKUP_SNAPSHOT &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public BackupSnapshotTask update(Protos.TaskState state) {
        return new BackupSnapshotTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public BackupSnapshotStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return BackupSnapshotStatus.create(builder
                .setData(CassandraData.createBackupSnapshotStatusData().getBytes())
                .setState(state)
                .build());
    }


    public BackupRestoreContext getBackupRestoreContext() {
        return getData().getBackupRestoreContext();
    }
}
