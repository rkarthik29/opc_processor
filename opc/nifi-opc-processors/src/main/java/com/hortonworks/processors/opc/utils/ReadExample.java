package com.hortonworks.processors.opc.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class ReadExample implements ClientExample {

    public static void main(String[] args) throws Exception {
        ReadExample example = new ReadExample();

        ClientExampleRunner t1 = new ClientExampleRunner(example);
        t1.start();
        t1.join();
        
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
        // synchronous connect
        client.connect().get();

        // synchronous read request via VariableNode
        //client.getAddressSpace().browseNode(new node);
        NodeId nodeid = new NodeId(5,85);
        client.getAddressSpace().browse(nodeid).thenApply(nodeList ->{
        	for(Node node:nodeList){
        		try {
					logger.info("{} Node={}", node.getBrowseName().get().getName());
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        		
        	}
        	 return future.complete(client);
        });
//        NodeId nodeid = new NodeId(5, "Sawtooth1");
//        VariableNode node = client.getAddressSpace().createVariableNode(nodeid);
//        DataValue value = node.readValue().get();
//
//        logger.info("StartTime={}", value.getValue().getValue());
//
//        // asynchronous read request
//        readServerStateAndTime(client).thenAccept(values -> {
//            DataValue v0 = values.get(0);
//            DataValue v1 = values.get(1);
//
//            logger.info("State={}", ServerState.from((Integer) v0.getValue().getValue()));
//            logger.info("CurrentTime={}", v1.getValue().getValue());
//
//            future.complete(client);
//        });
    }

    private CompletableFuture<List<DataValue>> readServerStateAndTime(OpcUaClient client) {
        List<NodeId> nodeIds = ImmutableList.of(
            Identifiers.Server_ServerStatus_State,
            Identifiers.Server_ServerStatus_CurrentTime);

        return client.readValues(0.0, TimestampsToReturn.Both, nodeIds);
    }

}