package com.mesosphere.dcos.cassandra.common.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Objects;

public class ExecutorConfig {
    public static Serializer<ExecutorConfig> JSON_SERIALIZER =
            new Serializer<ExecutorConfig>() {
                @Override
                public byte[] serialize(ExecutorConfig value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException(
                                "Error writing ExecutorConfig to JSON",
                                ex);
                    }
                }

                @Override
                public ExecutorConfig deserialize(byte[] bytes)
                        throws SerializationException {

                    try {
                        return JsonUtils.MAPPER.readValue(bytes,
                                ExecutorConfig.class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "ExecutorConfig form JSON", ex);
                    }
                }
            };

    public static ExecutorConfig create(
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int heapMb,
            int apiPort,
            String networkMode,
            String cniNetwork,
            String javaHome,
            URI jreLocation,
            URI executorLocation,
            URI cassandraLocation,
            URI dvdcli,
            String volumeDriver,
            String volumeName,
            String hostPath,
            String containerPath) {

        return new ExecutorConfig(
                command,
                arguments,
                cpus,
                memoryMb,
                heapMb,
                apiPort,
                networkMode,
                cniNetwork,
                javaHome,
                jreLocation,
                executorLocation,
                cassandraLocation,
                dvdcli,
                volumeDriver,
                volumeName,
                hostPath,
                containerPath);
    }

    @JsonCreator
    public static ExecutorConfig create(
            @JsonProperty("command") String command,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("cpus") double cpus,
            @JsonProperty("memory_mb") int memoryMb,
            @JsonProperty("heap_mb") int heapMb,
            @JsonProperty("api_port") int apiPort,
            @JsonProperty("network_mode") String networkMode,
            @JsonProperty("cni_network") String cniNetwork,
            @JsonProperty("java_home") String javaHome,
            @JsonProperty("jre_location") String jreLocation,
            @JsonProperty("executor_location") String executorLocation,
            @JsonProperty("cassandra_location") String cassandraLocation,
            @JsonProperty("emc_ecs_workaround") boolean emcEcsWorkaround,
            @JsonProperty("dvdcli") String dvdcli,
            @JsonProperty("volume_driver") String volumeDriver,
            @JsonProperty("volume_name") String volumeName,
            @JsonProperty("host_path") String hostPath,
            @JsonProperty("container_path") String containerPath)
            throws URISyntaxException, UnsupportedEncodingException {

        ExecutorConfig config = create(
                command,
                arguments,
                cpus,
                memoryMb,
                heapMb,
                apiPort,
                networkMode,
                cniNetwork,
                javaHome,
                URI.create(jreLocation),
                URI.create(executorLocation),
                URI.create(cassandraLocation),
                URI.create(dvdcli),
                volumeDriver,
                volumeName,
                hostPath,
                containerPath);

        return config;
    }

    @JsonProperty("command")
    private final String command;

    @JsonProperty("arguments")
    private final List<String> arguments;

    @JsonProperty("cpus")
    private final double cpus;

    @JsonProperty("memory_mb")
    private final int memoryMb;

    @JsonProperty("heap_mb")
    private final int heapMb;

    @JsonProperty("api_port")
    private final int apiPort;

    private final URI dvdcli;

    @JsonProperty("network_mode")
    private final String networkMode;

    @JsonProperty("cni_network")
    private final String cniNetwork;

    private final URI jreLocation;
    private final URI executorLocation;
    private final URI cassandraLocation;

    @JsonProperty("java_home")
    private final String javaHome;

    @JsonProperty("volume_name")
    private final String volumeName;

    @JsonProperty("volume_driver")
    private final String volumeDriver;

    @JsonProperty("host_path")
    private final String hostPath;

    @JsonProperty("container_path")
    private final String containerPath;

    public ExecutorConfig(
            String command,
            List<String> arguments,
            double cpus,
            int memoryMb,
            int heapMb,
            int apiPort,
            String networkMode,
            String cniNetwork,
            String javaHome,
            URI jreLocation,
            URI executorLocation,
            URI cassandraLocation,
            URI dvdcli,
            String volumeDriver,
            String volumeName,
            String hostPath,
            String containerPath) {

        this.command = command;
        this.arguments = arguments;
        this.cpus = cpus;
        this.memoryMb = memoryMb;
        this.heapMb = heapMb;
        this.apiPort = apiPort;
        this.networkMode = networkMode;
        this.cniNetwork = cniNetwork;
        this.jreLocation = jreLocation;
        this.executorLocation = executorLocation;
        this.cassandraLocation = cassandraLocation;
        this.dvdcli = dvdcli;
        this.volumeName = volumeName;
        this.volumeDriver = volumeDriver;
        this.javaHome = javaHome;
        this.hostPath = hostPath;
        this.containerPath = containerPath;
    }


    public int getApiPort() {
        return apiPort;
    }

    public String getNetworkMode() {
        return networkMode;
    }

    public String getCniNetwork() {
        return cniNetwork;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public URI getCassandraLocation() {
        return cassandraLocation;
    }

    public String getCommand() {
        return command;
    }

    public double getCpus() {
        return cpus;
    }

    public URI getExecutorLocation() {
        return executorLocation;
    }

    public int getHeapMb() {
        return heapMb;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public URI getJreLocation() {
        return jreLocation;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public String getHostPath(){ return  hostPath; }

    public String getContainerPath(){ return containerPath; }

    @JsonProperty("jre_location")
    public String getJreLocationString() {
        return jreLocation.toString();
    }

    @JsonProperty("executor_location")
    public String getExecutorLocationString() {
        return executorLocation.toString();
    }

    @JsonProperty("cassandra_location")
    public String getCassandraLocationString() {
        return cassandraLocation.toString();
    }

    @JsonProperty("dvdcli")
    public String getDvdcliString() {
        return dvdcli.toString();
    }

    public String getVolumeName() {
        return volumeName;
    }

    public String getVolumeDriver() {
        return volumeDriver;
    }

    @JsonIgnore
    public Set<String> getURIs() {
        Set<String> uris = new HashSet<String>();
        uris.add(dvdcli.toString());
        uris.add(executorLocation.toString());
        uris.add(cassandraLocation.toString());
        uris.add(jreLocation.toString());

        return uris;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorConfig)) return false;
        ExecutorConfig that = (ExecutorConfig) o;
        return Double.compare(that.getCpus(), getCpus()) == 0 &&
                getMemoryMb() == that.getMemoryMb() &&
                getHeapMb() == that.getHeapMb() &&
                getApiPort() == that.getApiPort() &&
                Objects.equals(getCommand(), that.getCommand()) &&
                Objects.equals(getArguments(), that.getArguments()) &&
                Objects.equals(getJreLocation(), that.getJreLocation()) &&
                Objects.equals(getExecutorLocation(),
                        that.getExecutorLocation()) &&
                Objects.equals(getCassandraLocation(),
                        that.getCassandraLocation()) &&
                Objects.equals(getJavaHome(), that.getJavaHome())
                && getHostPath().equals(that.getHostPath())
                && getContainerPath().equals(that.getContainerPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCommand(), getArguments(), getCpus(),
                getMemoryMb(),
                getHeapMb(), getApiPort(),
                getJreLocation(), getExecutorLocation(), getCassandraLocation(),
                getJavaHome(), getContainerPath(), getHostPath());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
