package com.xin.projects.amq.clustering;


import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.network.DemandForwardingBridge;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkBridgeConfiguration;
import org.apache.activemq.network.NetworkBridgeFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xin on 2015/1/26.
 */
public class EmbedBroker implements PoolHandler.PoolListener {
    private static final Logger logger = LoggerFactory.getLogger(EmbedBroker.class);
    private BrokerService broker;
    private String localUrl = "tcp://localhost:61616";
    private String localName = "broker1";
    private Map<String, NetworkBridge> bridges;

    public EmbedBroker(String localName, String localUrl) {
        this.localName = localName;
        this.localUrl = localUrl;
        bridges = new HashMap<String, NetworkBridge>();
    }

    public BrokerService createBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName(localName);
        // String broker
        ManagementContext managementContext = broker.getManagementContext();
        managementContext.setFindTigerMbeanServer(true);
        managementContext.setUseMBeanServer(true);
        managementContext.setCreateConnector(true);
        broker.setManagementContext(managementContext);
        broker.setUseJmx(true);
        broker.addConnector(localUrl);
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    public void setup() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        broker.start();
        broker.waitUntilStarted();
    }

    @Override
    public synchronized void changes(Map<String, String> urls) {
//        for (Map.Entry<String, String> s : urls.entrySet()) {
//            System.out.println(s.getKey() + "||" + s.getValue());
//        }
        try {
            HashMap<String, NetworkBridge> temp = new HashMap<String, NetworkBridge>();
            for (Map.Entry<String, String> entry : urls.entrySet()) {
                if (entry.getKey().equals(localName) && entry.getValue().equals(localUrl)) {
                    continue;
                }
                if (bridges.get(entry.getKey()) != null) {
                    temp.put(entry.getKey(), bridges.get(entry.getKey()));
                    bridges.remove(entry.getKey());
                } else {
                    NetworkBridge bridge = createBridge(broker, localUrl, entry.getValue());
                    bridge.start();
                    System.out.println("start: " + localUrl + "----" + entry.getValue());

                    temp.put(entry.getKey(), bridge);
                }
            }
            for (Map.Entry<String, NetworkBridge> entry : bridges.entrySet()) {
                entry.getValue().stop();
                System.out.println("stop: " + localUrl + "----" + entry.getValue());

            }
            bridges = temp;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            logger.error("bridge start error", e);
        }

    }


    protected Transport createTransport(String url) throws Exception {
        Transport transport = TransportFactory.connect(new URI(url));
        return transport;
    }

    public NetworkBridge createBridge(BrokerService broker, String localUrlurl, String remoteUrl) throws Exception {
        DemandForwardingBridge bridge;
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        config.setBrokerName(broker.getBrokerName());
        config.setDispatchAsync(true);
        config.setDuplex(true);

        Transport localTransport = createTransport(localUrlurl);
        Transport remoteTransport = createTransport(remoteUrl);

        // Create a network bridge between the two brokers
        bridge = NetworkBridgeFactory.createBridge(config, localTransport, remoteTransport);
        bridge.setBrokerService(broker);
        return bridge;
    }

    public BrokerService getBroker() {
        return broker;
    }

    public void setBroker(BrokerService broker) {
        this.broker = broker;
    }


}
