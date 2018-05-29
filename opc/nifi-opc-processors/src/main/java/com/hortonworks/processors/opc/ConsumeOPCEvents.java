/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.processors.opc;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;

import net.minidev.json.JSONObject;

@Tags({"opc"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConsumeOPCEvents extends AbstractProcessor {

    public static final PropertyDescriptor OPC_SERVER_URL = new PropertyDescriptor
            .Builder().name("OPC_SERVER_URL")
            .displayName("SERVER URL for OPC")
            .description("opc.tcp://host:port/server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor NAME_SPACE_INDEX = new PropertyDescriptor
            .Builder().name("NameSpaceIndex")
            .displayName("Name Space Index for the identifier")
            .description("")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IDENTIFIER = new PropertyDescriptor
            .Builder().name("Identifier")
            .displayName("Identifier for the metric")
            .description("")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SUB_DUR = new PropertyDescriptor
            .Builder().name("SUB_DUR")
            .displayName("Duration for the subscription, how long to listen for response")
            .description("")
            .required(true)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private ComponentLog logger;
    
    private final BlockingQueue<HashMap<String, Object>> dataPoints = new LinkedBlockingQueue<HashMap<String, Object>>();
    
    private OpcUaClient client;
    
    private final AtomicLong clientHandles = new AtomicLong(1L);
    
    private final CompletableFuture<OpcUaClient> future = new CompletableFuture<>();
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger=getLogger();
    	final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OPC_SERVER_URL);
        descriptors.add(NAME_SPACE_INDEX);
        descriptors.add(IDENTIFIER);
        descriptors.add(SUB_DUR);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	 String opc_url = context.getProperty(OPC_SERVER_URL).getValue();
    	 try {
			client = createClient(opc_url);
			client.connect().get();		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("subscription error",e);
		}
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	logger.info("Starting listening for opc events");
    	 String opc_url = context.getProperty(OPC_SERVER_URL).getValue();
         int nameSpaceIndex=context.getProperty(NAME_SPACE_INDEX).asInteger();
         String identifiers = context.getProperty(IDENTIFIER).getValue();
         Long duration = context.getProperty(SUB_DUR).asLong();
         try {	
			// create a subscription @ 1000ms
	        UaSubscription subscription = client.getSubscriptionManager().createSubscription(10.0).get();
	        ArrayList<MonitoredItemCreateRequest> requests = new ArrayList<MonitoredItemCreateRequest>();
	        //ArrayList<String> identifiers = new ArrayList<String>();
	        //identifiers.add("Sawtooth1");
	        //identifiers.add("Sinusoid1");
	        for(String identifier : identifiers.split(",")){
	        
	        NodeId node = new NodeId(5, identifier);
	        // subscribe to the Value attribute of the server's CurrentTime node
	        ReadValueId readValueId = new ReadValueId(
	            node,
	            AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);

	        // important: client handle must be unique per item
	        UInteger clientHandle = uint(clientHandles.getAndIncrement());

	        MonitoringParameters parameters = new MonitoringParameters(
	            clientHandle,
	            10.0,     // sampling interval
	            null,       // filter, null means use default
	            uint(10),   // queue size
	            true        // discard oldest
	        );

	        MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
	            readValueId, MonitoringMode.Reporting, parameters);
	        requests.add(request);
	        }
	        // when creating items in MonitoringMode.Reporting this callback is where each item needs to have its
	        // value/event consumer hooked up. The alternative is to create the item in sampling mode, hook up the
	        // consumer after the creation call completes, and then change the mode for all items to reporting.
	        BiConsumer<UaMonitoredItem, Integer> onItemCreated =
	            (item, id) -> item.setValueConsumer(this::onSubscriptionValue);
	        
	        //for(MonitoredItemCreateRequest request : requests){    
	        List<UaMonitoredItem> items = subscription.createMonitoredItems(
	            TimestampsToReturn.Both,
	            newArrayList(requests),
	            onItemCreated
	        ).get();
	        

	        for (UaMonitoredItem item : items) {
	            if (item.getStatusCode().isGood()) {
	                logger.info("item created for nodeId={}",new Object[]{item.getReadValueId().getNodeId()});
	            } else {
	               logger.warn(
	                   "failed to create item for nodeId={} (status={})",
	                   new Object[]{ item.getReadValueId().getNodeId(), item.getStatusCode()});
	            }
	        }
	        while(isScheduled()){
	        HashMap<String,Object> dataPoint = dataPoints.poll(50, TimeUnit.MICROSECONDS);
	        if(dataPoint==null)
	        	continue;
	        FlowFile flowFile=session.create();
		    JSONObject data = new JSONObject(dataPoint);
		        if (data.toJSONString().length() > 0) {
		            flowFile = session.write(flowFile, new OutputStreamCallback() {
		                @Override
		                public void process(final OutputStream out) throws IOException {
		                    out.write(data.toJSONString().getBytes());
		                }
		            });
		        }
		        flowFile = session.putAttribute(flowFile,"Identifier",dataPoint.get("Identifier").toString() );
		        flowFile = session.putAttribute(flowFile, "host", opc_url);
		        session.getProvenanceReporter().create(flowFile);
		        session.transfer(flowFile, SUCCESS);
		        session.commit();
	        }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("subscription error",e);
		}
         
        // TODO implement
    }
    
    private OpcUaClient createClient(String url) throws Exception {
        File securityTempDir = new File(System.getProperty("java.io.tmpdir"), "security");
        if (!securityTempDir.exists() && !securityTempDir.mkdirs()) {
            throw new Exception("unable to create security dir: " + securityTempDir);
        }
       // LoggerFactory.getLogger(getClass())
       //     .info("security temp dir: {}", securityTempDir.getAbsolutePath());
        
        //KeyStoreLoader loader = new KeyStoreLoader().load(securityTempDir);
        
        SecurityPolicy securityPolicy =  SecurityPolicy.None;

        EndpointDescription[] endpoints;

        try {
            endpoints = UaTcpStackClient
                .getEndpoints(url)
                .get();
        } catch (Throwable ex) {
            // try the explicit discovery endpoint as well
            String discoveryUrl = url + "/discovery";
            //logger.info("Trying explicit discovery URL: {}", discoveryUrl);
            endpoints = UaTcpStackClient
                .getEndpoints(discoveryUrl)
                .get();
        }

        EndpointDescription endpoint = Arrays.stream(endpoints)
            .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
            .findFirst().orElseThrow(() -> new Exception("no desired endpoints returned"));

        //logger.info("Using endpoint: {} [{}]", endpoint.getEndpointUrl());

        OpcUaClientConfig config = OpcUaClientConfig.builder()
            .setApplicationName(LocalizedText.english("eclipse milo opc-ua client"))
            .setApplicationUri("urn:eclipse:milo:examples:client")
            //.setCertificate(loader.getClientCertificate())
            //.setKeyPair(loader.getClientKeyPair())
            .setEndpoint(endpoint)
            .setMaxResponseMessageSize(uint(10))
            .setIdentityProvider(new AnonymousProvider())
            .setRequestTimeout(uint(5000000))
            .build();

        return new OpcUaClient(config);
    }
    
    private void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
        //logger.warn(
         //   "subscription value received: item="+item.getReadValueId().getNodeId()+", value="+value.getValue());
        //logger.info(""+value.getSourceTime());
    	final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    	final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
    	final TimeZone utc = TimeZone.getTimeZone("UTC");
    	sdf.setTimeZone(utc);
        HashMap<String,Object> dataPoint = new HashMap<String,Object>();
        dataPoint.put("namespaceindex", item.getReadValueId().getNodeId().getNamespaceIndex());
        dataPoint.put("Identifier", item.getReadValueId().getNodeId().getIdentifier());
        dataPoint.put("timestamp", sdf.format(value.getSourceTime().getJavaDate()));
        dataPoint.put("value", value.getValue().getValue());
        try {
			dataPoints.put(dataPoint);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
