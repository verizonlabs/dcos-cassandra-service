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
package com.mesosphere.dcos.cassandra.common.tasks;

import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.dcos.DcosCluster;
import org.apache.mesos.executor.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static com.mesosphere.dcos.cassandra.common.util.TaskUtils.*;

/**
 * CassandraTaskExecutor aggregates the executor information for CassandraTasks.
 * It is associated with CassandraTasks prior to launching them. Only one
 * executor should exist on a slave at a time. This executor should be
 * created when launching the CassandraDaemonTask on that slave, and it
 * should be reused by Cluster tasks that operate on the Daemon.
 */
public class CassandraTaskExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTaskExecutor.class);
    private static final String CNI_NETWORK = "CNI";

    /**
     * Creates a new CassandraTaskExecutor.
     *
     * @param frameworkId The id of the executor's framework.
     * @return A new CassandraTaskExecutor constructed from the parameters.
     */
    public static CassandraTaskExecutor create(
            final String frameworkId,
            final String name,
            final String role,
            final String principal,
            final ExecutorConfig config) {

        return new CassandraTaskExecutor(
            frameworkId,
            name,
            role,
            principal,
            config);
    }

    /**
     * Parses a CassandraTaskExecutor from a Protocol Buffers representation.
     *
     * @param info The ExecutorInfo that contains a CassandraTaskExecutor.
     * @return A CassandraTaskExecutor parsed from info.
     */
    public static final CassandraTaskExecutor parse(
            final Protos.ExecutorInfo info) {
        return new CassandraTaskExecutor(info);
    }


    private Protos.ExecutorInfo info;

    /**
     * Constructs a CassandraTaskExecutor.
     *
     * @param frameworkId The id of the executor's framework.
     * @param name        The name of the executor.
     */
    private CassandraTaskExecutor(
        String frameworkId,
        String name,
        String role,
        String principal,
        ExecutorConfig config) {

        Protos.ExecutorInfo.Builder executorBuilder = Protos.ExecutorInfo.newBuilder();
        String commandString = config.getCommand();
        String volumeName = name.replace("node-", config.getVolumeName() + "_").replace("_executor", "");

        Map<String, String> map = new HashMap<>();
        map.put("JAVA_HOME", config.getJavaHome());
        map.put("JAVA_OPTS", "-Xmx" + config.getHeapMb() + "M");
        map.put("EXECUTOR_API_PORT", Integer.toString(config.getApiPort()));

        Capabilities capabilities = new Capabilities(new DcosCluster());

        Protos.ContainerInfo.Builder containerInfo = Protos.ContainerInfo.newBuilder()
                .setType(Protos.ContainerInfo.Type.MESOS);

        if (config.getVolumeDriver().equalsIgnoreCase("rexray")){
            commandString = setRexrayCommand(volumeName, config);
            containerInfo = setRexrayContainerOptions(containerInfo, volumeName);
        }

        try {
            if (capabilities.supportsNamedVips() && CNI_NETWORK.equalsIgnoreCase(config.getNetworkMode())) {
                containerInfo
                        .addNetworkInfos(Protos.NetworkInfo.newBuilder()
                        .setName(config.getCniNetwork()));
            }
        } catch (IOException | URISyntaxException e) {
            LOGGER.error("Unable to detect named VIP support: {}", e);
        } finally {
            executorBuilder.setFrameworkId(Protos.FrameworkID.newBuilder()
                    .setValue(frameworkId))
                    .setName(name)
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue(""))
                    .setContainer(containerInfo)
                    .setCommand(createCommandInfo(commandString,
                            config.getArguments(),
                            config.getURIs(),
                            map))
                    .addAllResources(
                            Arrays.asList(
                                    createCpus(config.getCpus(), role, principal),
                                    createMemoryMb(config.getMemoryMb(), role, principal),
                                    createPorts(Arrays.asList(config.getApiPort()), role, principal)));
            this.info = executorBuilder.build();
        }
    }

    CassandraTaskExecutor(final Protos.ExecutorInfo info) {
        this.info = info;
    }

    private String setRexrayCommand(String volumeName, ExecutorConfig config){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("./dvdcli mount --volumename=");
        stringBuilder.append(volumeName);
        stringBuilder.append(" --volumedriver=");
        stringBuilder.append(config.getVolumeDriver().trim());
        stringBuilder.append(" && ");
        stringBuilder.append(config.getCommand());

        return stringBuilder.toString();
    }

    private Protos.ContainerInfo.Builder setRexrayContainerOptions(Protos.ContainerInfo.Builder builder, String volumeName) {
        return builder
                .setType(Protos.ContainerInfo.Type.MESOS)
                .addVolumes(Protos.Volume.newBuilder().setSource(
                        Protos.Volume.Source.newBuilder()
                        .setDockerVolume(Protos.Volume.Source.DockerVolume.newBuilder()
                            .setDriver("rexray")
                            .setName(volumeName)
                            .build())
                            .setType(Protos.Volume.Source.Type.DOCKER_VOLUME)
                        .build()
                        )
                .setMode(Protos.Volume.Mode.RW)
                .setContainerPath(CassandraConfig.VOLUME_PATH)
                );
    }

    public String getName() {
        return info.getName();
    }


    public Set<String> getURIs() {
        return toSet(info.getCommand().getUrisList());
    }

    /**
     * Gets the API port.
     *
     * @return The port the executor's API will listen on.
     */
    public int getApiPort() {
        return Integer.parseInt(
                getValue("EXECUTOR_API_PORT", info.getCommand()
                        .getEnvironment()));
    }

    /**
     * Gets the command.
     *
     * @return The command used to launch the executor.
     */
    public String getCommand() {
        return info.getCommand().getValue();
    }

    public String getRole(){
        return info.getResources(0).getRole();
    }

    public String getPrincipal(){
        return info.getResources(0).getReservation().getPrincipal();
    }
    /**
     * Gets the cpu shares.
     *
     * @return The cpu shares allocated to the executor.
     */
    public double getCpus() {
        return getResourceCpus(info.getResourcesList());
    }

    /**
     * Gets the heap size.
     *
     * @return The size of the executor's JVM heap in Mb.
     */
    public int getHeapMb() {
        return Integer.parseInt(
                getValue("JAVA_OPTS", info.getCommand().getEnvironment())
                        .replace("-Xmx", "")
                        .replace("M", ""));

    }

    /**
     * Gets the executors id.
     *
     * @return The universally unique identifier for the executor.
     */
    public String getId() {
        return info.getExecutorId().getValue();
    }

    /**
     * Gets the Java home.
     *
     * @return The location of the executor's local Java installation.
     */
    public String getJavaHome() {
        return getValue("JAVA_HOME", info.getCommand().getEnvironment());
    }

    /**
     * Gets the executors memory allocation.
     *
     * @return The memory allocated to the executor in Mb.
     */
    public int getMemoryMb() {
        return getResourceMemoryMb(info.getResourcesList());
    }


    /**
     * Gets a Protocol Buffers representation of the executor.
     */
    public Protos.ExecutorInfo getExecutorInfo() {
        return info;
    }

    public CassandraTaskExecutor withNewId() {
        return parse(
                Protos.ExecutorInfo.newBuilder(getExecutorInfo())
                        .setExecutorId(ExecutorUtils.toExecutorId(getName())).build());
    }

    public CassandraTaskExecutor clearId() {
        return parse(
                Protos.ExecutorInfo.newBuilder(getExecutorInfo())
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue("")).build());
    }

    public boolean matches(final ExecutorConfig config) {
        return Double.compare(getCpus(), config.getCpus()) == 0 &&
                Objects.equals(getCommand(), config.getCommand()) &&
                Sets.difference(getURIs(), new HashSet<>(config.getURIs()))
                        .isEmpty() &&
                getHeapMb() == config.getHeapMb() &&
                getHeapMb() == config.getHeapMb();
    }

    public CassandraTaskExecutor update(final ExecutorConfig config) {
        return new CassandraTaskExecutor(
                Protos.ExecutorInfo.newBuilder(info)
                        .setExecutorId(ExecutorUtils.toExecutorId(info.getName()))
                        .addAllResources(updateResources(config.getCpus(), config
                                        .getMemoryMb(),
                                info.getResourcesList())).build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraTaskExecutor)) return false;
        CassandraTaskExecutor that = (CassandraTaskExecutor) o;
        return this.info.equals(that.info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.info);
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(this.info);
    }
}
