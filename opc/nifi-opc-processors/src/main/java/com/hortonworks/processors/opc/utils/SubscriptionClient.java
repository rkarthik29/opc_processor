package com.hortonworks.processors.opc.utils;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionClient implements ClientExample {

    public static void main(String[] args) throws Exception {
        SubscriptionClient example = new SubscriptionClient();

        ClientExampleRunner runner = new ClientExampleRunner(example);
        runner.start();
        runner.join();
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicLong clientHandles = new AtomicLong(1L);

    @Override
    public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
        // synchronous connect
        client.connect().get();

        // create a subscription @ 1000ms
        List<NodeId> nodes = new ArrayList<NodeId>();
        browseNode("", client, Identifiers.RootFolder, nodes);

        UaSubscription subscription = client.getSubscriptionManager().createSubscription(10.0).get();
        ArrayList<MonitoredItemCreateRequest> requests = new ArrayList<MonitoredItemCreateRequest>();
        ArrayList<String> identifiers = new ArrayList<String>();
        identifiers.add("Sawtooth1");
        identifiers.add("Sinusoid1");
        for (NodeId node : nodes) {
            if (node.getNamespaceIndex().intValue() != 5)
                continue;
            // NodeId node = new NodeId(5, identifier);
            // subscribe to the Value attribute of the server's CurrentTime node
            ReadValueId readValueId = new ReadValueId(node, AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);

            // important: client handle must be unique per item
            UInteger clientHandle = uint(clientHandles.getAndIncrement());

            MonitoringParameters parameters = new MonitoringParameters(clientHandle, 1.0, // sampling
                                                                                          // interval
                    null, // filter, null means use default
                    uint(10), // queue size
                    true // discard oldest
            );

            MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(readValueId, MonitoringMode.Reporting,
                    parameters);
            requests.add(request);
        }
        // when creating items in MonitoringMode.Reporting this callback is
        // where each item needs to have its
        // value/event consumer hooked up. The alternative is to create the item
        // in sampling mode, hook up the
        // consumer after the creation call completes, and then change the mode
        // for all items to reporting.
        BiConsumer<UaMonitoredItem, Integer> onItemCreated = (item, id) -> item
                .setValueConsumer(this::onSubscriptionValue);

        // for(MonitoredItemCreateRequest request : requests){
        List<UaMonitoredItem> items = subscription
                .createMonitoredItems(TimestampsToReturn.Both, newArrayList(requests), onItemCreated).get();

        for (UaMonitoredItem item : items) {
            if (item.getStatusCode().isGood()) {
                logger.info("item created for nodeId={}", item.getReadValueId().getNodeId());
            } else {
                logger.warn("failed to create item for nodeId={} (status={})", item.getReadValueId().getNodeId(),
                        item.getStatusCode());
            }
        }
        // }

        // let the example run for 5 seconds then terminate
        // Thread.sleep(5000);
        // \=
        // future.complete(client);
        future.join();
    }

    private void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
        System.out.println("subscription value received: item={}, value={}" + item.getReadValueId().getNodeId()
                + value.getValue());
        logger.info("" + value.getSourceTime());
    }

    private void browseNode(String indent, OpcUaClient client, NodeId browseRoot, List<NodeId> nodes) {
        try {
            List<Node> childNodes = client.getAddressSpace().browse(browseRoot).get();
            for (Node node : childNodes) {
                logger.info("{} Node={}", indent, node.getBrowseName().get().getName());
                nodes.add(node.getNodeId().get());
                // recursively browse to children
                browseNode(indent + "  ", client, node.getNodeId().get(), nodes);
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Browsing nodeId={} failed: {}", browseRoot, e.getMessage(), e);
        }
    }

}