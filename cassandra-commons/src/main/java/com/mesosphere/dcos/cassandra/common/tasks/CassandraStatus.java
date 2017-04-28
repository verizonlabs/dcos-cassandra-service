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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

/**
 * CassandraStatus is the status object sent from the Executor to the Scheduler
 * to track the status of a Cassandra Daemon.
 */
public class CassandraStatus {

    @JsonProperty("mode")
    private final CassandraMode mode;
    @JsonProperty("joined")
    private final boolean joined;
    @JsonProperty("rpc_running")
    private final boolean rpcRunning;
    @JsonProperty("native_transport_running")
    private final boolean nativeTransportRunning;
    @JsonProperty("gossip_initialized")
    private final boolean gossipInitialized;
    @JsonProperty("gossip_running")
    private final boolean gossipRunning;
    @JsonProperty("host_id")
    private final String hostId;
    @JsonProperty("endpoint")
    private final String endpoint;
    @JsonProperty("token_count")
    private final int tokenCount;
    @JsonProperty("data_center")
    private final String dataCenter;
    @JsonProperty("rack")
    private final String rack;
    @JsonProperty("version")
    private final String version;

    /**
     * Creates a CassandraStatus.
     * @param mode The mode of the Cassandra node.
     * @param joined True if the node has joined the cluster.
     * @param rpcRunning True if the node has rpc running.
     * @param nativeTransportRunning True if the node has CQL transport running.
     * @param gossipInitialized True if gossip with the cluster has been
     *                          initialized.
     * @param gossipRunning True if the node is participating in gossip.
     * @param hostId The id of the node in the ring.
     * @param endpoint The node's endpoint identifier.
     * @param tokenCount The number of tokens assigned to the node.
     * @param dataCenter The datacenter for the node.
     * @param rack The rack for the node.
     * @param version The version of Cassandra the node is running.
     * @return A CassandraStatus constructed from the parameters.
     */
    @JsonCreator
    public static CassandraStatus create(
            @JsonProperty("mode") final CassandraMode mode,
            @JsonProperty("joined") final boolean joined,
            @JsonProperty("rpc_running") final boolean rpcRunning,
            @JsonProperty("native_transport_running")
            final boolean nativeTransportRunning,
            @JsonProperty("gossip_initialized") final boolean gossipInitialized,
            @JsonProperty("gossip_running") final boolean gossipRunning,
            @JsonProperty("host_id") final String hostId,
            @JsonProperty("endpoint") final String endpoint,
            @JsonProperty("token_count") final int tokenCount,
            @JsonProperty("data_center") final String dataCenter,
            @JsonProperty("rack") final String rack,
            @JsonProperty("version") final String version) {

        return new CassandraStatus(
                mode,
                joined,
                rpcRunning,
                nativeTransportRunning,
                gossipInitialized,
                gossipRunning,
                hostId,
                endpoint,
                tokenCount,
                dataCenter,
                rack,
                version);
    }


    /**
     * Constructs a CassandraStatus.
     * @param mode The mode of the Cassandra node.
     * @param joined True if the node has joined the cluster.
     * @param rpcRunning True if the node has rpc running.
     * @param nativeTransportRunning True if the node has CQL transport running.
     * @param gossipInitialized True if gossip with the cluster has been
     *                          initialized.
     * @param gossipRunning True if the node is participating in gossip.
     * @param hostId The id of the node in the ring.
     * @param endpoint The node's endpoint identifier.
     * @param tokenCount The number of tokens assigned to the node.
     * @param dataCenter The datacenter for the node.
     * @param rack The rack for the node.
     * @param version The version of Cassandra the node is running.
     */
    public CassandraStatus(
            CassandraMode mode,
            boolean joined,
            boolean rpcRunning,
            boolean nativeTransportRunning,
            boolean gossipInitialized,
            boolean gossipRunning,
            String hostId,
            String endpoint,
            int tokenCount,
            String dataCenter,
            String rack,
            String version) {
        this.mode = mode;
        this.joined = joined;
        this.rpcRunning = rpcRunning;
        this.nativeTransportRunning = nativeTransportRunning;
        this.gossipInitialized = gossipInitialized;
        this.gossipRunning = gossipRunning;
        this.hostId = hostId;
        this.endpoint = endpoint;
        this.tokenCount = tokenCount;
        this.dataCenter = dataCenter;
        this.rack = rack;
        this.version = version;
    }

    /**
     * Gets the mode.
     * @return The mode of the Cassandra node.
     */
    private CassandraMode getMode() {
        return mode;
    }

    /**
     * Gets the Cassandra version.
     * @return The version of Cassandra the node is running.
     */
    private String getVersion() {
        return version;
    }

    /**
     * Test if the node is joined.
     * @return True if the node has joined the ring.
     */
    private boolean isJoined() {
        return joined;
    }

    /**
     * Tests if RPC is running.
     * @return True if the node is running RPS transport.
     */
    private boolean isRpcRunning() {
        return rpcRunning;
    }

    /**
     * Tests if CQL is running
     * @return True if the node is running CQL transport.
     */
    private boolean isNativeTransportRunning() {
        return nativeTransportRunning;
    }

    /**
     * Tests if the gossip is initialized.
     * @return True if the node has initialized Gossip.
     */
    private boolean isGossipInitialized() {
        return gossipInitialized;
    }

    /**
     * Tests if gossip is running.
     * @return True if the node is gossiping within its cluster.
     */
    private boolean isGossipRunning() {
        return gossipRunning;
    }

    /**
     * Gets the node's host id.
     * @return The unique identifier of the node's host in the ring.
     */
    private String getHostId() {
        return hostId;
    }

    /**
     * Gets the node's endpoint.
     * @return The endpoint assigned to the node in the ring.
     */
    private String getEndpoint() {
        return endpoint;
    }

    /**
     * Gets the token count.
     * @return The number of tokens assigned to the node.
     */
    private int getTokenCount() {
        return tokenCount;
    }

    /**
     * Gets the node's data center.
     * @return The data center in which the node is located.
     */
    private String getDataCenter() {
        return dataCenter;
    }

    /**
     * Gets the node's rack.
     * @return The rack on which the node is located.
     */
    private String getRack() {
        return rack;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraStatus)) return false;

        CassandraStatus that = (CassandraStatus) o;

        if (isJoined() != that.isJoined()) return false;
        if (isRpcRunning() != that.isRpcRunning()) return false;
        if (isNativeTransportRunning() != that.isNativeTransportRunning())
            return false;
        if (isGossipInitialized() != that.isGossipInitialized()) return false;
        if (isGossipRunning() != that.isGossipRunning()) return false;
        if (getTokenCount() != that.getTokenCount()) return false;
        if (getMode() != that.getMode()) return false;
        if (getHostId() != null ? !getHostId().equals(
                that.getHostId()) : that.getHostId() != null) return false;
        if (getEndpoint() != null ? !getEndpoint().equals(
                that.getEndpoint()) : that.getEndpoint() != null) return false;
        if (getDataCenter() != null ? !getDataCenter().equals(
                that.getDataCenter()) : that.getDataCenter() != null)
            return false;
        return (getRack() != null ? getRack().equals(
                that.getRack()) : that.getRack() == null) && (getVersion() != null ? getVersion().equals(that.getVersion()) : that.getVersion() == null);

    }

    @Override
    public int hashCode() {
        int result = getMode() != null ? getMode().hashCode() : 0;
        result = 31 * result + (isJoined() ? 1 : 0);
        result = 31 * result + (isRpcRunning() ? 1 : 0);
        result = 31 * result + (isNativeTransportRunning() ? 1 : 0);
        result = 31 * result + (isGossipInitialized() ? 1 : 0);
        result = 31 * result + (isGossipRunning() ? 1 : 0);
        result = 31 * result + (getHostId() != null ? getHostId().hashCode() : 0);
        result = 31 * result + (getEndpoint() != null ? getEndpoint().hashCode() : 0);
        result = 31 * result + getTokenCount();
        result = 31 * result + (getDataCenter() != null ? getDataCenter().hashCode() : 0);
        result = 31 * result + (getRack() != null ? getRack().hashCode() : 0);
        result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
