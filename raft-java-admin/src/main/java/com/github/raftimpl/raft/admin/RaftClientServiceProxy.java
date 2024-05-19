package com.github.raftimpl.raft.admin;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.raftimpl.raft.proto.RaftProto;
import com.github.raftimpl.raft.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * RaftClientServiceProxy acts as a proxy for client to interact with a Raft cluster.
 * It implements the RaftClientService interface and handles communication
 * with both the entire cluster and the current leader node.
 *
 * This class is not thread-safe and should be used carefully in multi-threaded environments.
 */
public class RaftClientServiceProxy implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceProxy.class);
    private static final JsonFormat jsonFormat = new JsonFormat();

    /** List of all servers in the Raft cluster */
    private List<RaftProto.Server> cluster;
    /** RPC client for communicating with the entire cluster */
    private RpcClient clusterRPCClient;
    /** Service interface for cluster-wide operations */
    private RaftClientService clusterRaftClientService;

    /** The current leader server */
    private RaftProto.Server leader;
    /** RPC client for communicating with the leader */
    private RpcClient leaderRPCClient;
    /** Service interface for leader-specific operations */
    private RaftClientService leaderRaftClientService;

    /** RPC client options for configuring connection parameters */
    private RpcClientOptions rpcClientOptions = new RpcClientOptions();

    /**
     * Constructs a new RaftClientServiceProxy.
     * Proxy is initialized with the IP addresses and ports of the Raft cluster servers.
     * Are used only to send requests to the leader.
     *
     * @param ipPorts A string containing IP addresses and ports of the Raft cluster servers,
     *                formatted as "10.1.1.1:8888,10.2.2.2:9999"
     */
    public RaftClientServiceProxy(String ipPorts) {
        // Configure RPC client options
        rpcClientOptions.setConnectTimeoutMillis(1000); // 1s connection timeout
        rpcClientOptions.setReadTimeoutMillis(3600000); // 1hour read timeout
        rpcClientOptions.setWriteTimeoutMillis(1000); // 1s write timeout

        // Initialize cluster RPC client and service
        clusterRPCClient = new RpcClient(ipPorts, rpcClientOptions);
        clusterRaftClientService = BrpcProxy.getProxy(clusterRPCClient, RaftClientService.class);

        // Update configuration to identify the current leader
        updateConfiguration();
    }

    /**
     * Retrieves the current leader of the Raft cluster by sending a request to the leader.
     *
     * @param request The GetLeaderRequest (unused in this implementation)
     * @return A GetLeaderResponse containing information about the current leader
     */
    @Override
    public RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request) {
        return clusterRaftClientService.getLeader(request);
    }

    /**
     * Retrieves the current configuration of the Raft cluster.
     *
     * @param request The GetConfigurationRequest (unused in this implementation)
     * @return A GetConfigurationResponse containing the current cluster configuration
     */
    @Override
    public RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request) {
        return clusterRaftClientService.getConfiguration(request);
    }

    /**
     * Adds new peers to the Raft cluster.
     * If the initial request fails due to a leader change, it updates the configuration and retries.
     *
     * @param request An AddPeersRequest containing the servers to be added
     * @return An AddPeersResponse indicating the result of the operation
     */
    @Override
    public RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request) {
        RaftProto.AddPeersResponse response = leaderRaftClientService.addPeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.addPeers(request);
        }
        return response;
    }

    /**
     * Removes peers from the Raft cluster.
     * If the initial request fails due to a leader change, it updates the configuration and retries.
     *
     * @param request A RemovePeersRequest containing the servers to be removed
     * @return A RemovePeersResponse indicating the result of the operation
     */
    @Override
    public RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request) {
        RaftProto.RemovePeersResponse response = leaderRaftClientService.removePeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfiguration();
            response = leaderRaftClientService.removePeers(request);
        }
        return response;
    }

    /**
     * Stops all RPC clients, cleaning up resources.
     * This method should be called when the proxy is no longer needed.
     */
    public void stop() {
        if (leaderRPCClient != null) {
            leaderRPCClient.stop();
        }
        if (clusterRPCClient != null) {
            clusterRPCClient.stop();
        }
    }

    /**
     * Updates the cluster configuration and leader information.
     * This method is called internally when a leader change is detected.
     *
     * @return true if the update was successful, false otherwise
     */
    private boolean updateConfiguration() {
        RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
        RaftProto.GetConfigurationResponse response = clusterRaftClientService.getConfiguration(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            if (leaderRPCClient != null) {
                leaderRPCClient.stop();
            }
            leader = response.getLeader();
            leaderRPCClient = new RpcClient(convertEndPoint(leader.getEndpoint()), rpcClientOptions);
            leaderRaftClientService = BrpcProxy.getProxy(leaderRPCClient, RaftClientService.class);
            return true;
        }
        return false;
    }

    /**
     * Converts a RaftProto.Endpoint to a com.baidu.brpc.client.instance.Endpoint.
     *
     * @param endPoint The RaftProto.Endpoint to convert
     * @return The converted Endpoint
     */
    private Endpoint convertEndPoint(RaftProto.Endpoint endPoint) {
        return new Endpoint(endPoint.getHost(), endPoint.getPort());
    }
}