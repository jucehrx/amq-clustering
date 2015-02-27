package com.xin.projects.amq.clustering;

import org.apache.activemq.command.Message;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkBridgeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xin on 2015/1/30.
 */
public class BridgeListener implements NetworkBridgeListener {
    private static final Logger logger = LoggerFactory.getLogger(BridgeListener.class);
    String key;
    String url;
    BridgeFailed listener;

    public BridgeListener(String key, String url, BridgeFailed listener) {
        this.key = key;
        this.url = url;
        this.listener = listener;
    }

    @Override
    public void bridgeFailed() {
        if (listener != null) {
            listener.processFailed(key, url);
        }
    }

    @Override
    public void onStart(NetworkBridge bridge) {
        System.out.println("Start: " + bridge.getLocalAddress() + "---->" + bridge.getRemoteAddress());
        logger.info("Start: " + bridge.getLocalAddress() + "---->" + bridge.getRemoteAddress());
    }

    @Override
    public void onStop(NetworkBridge bridge) {
        System.out.println("Stop: " + bridge.getLocalAddress() + "---->" + bridge.getRemoteAddress());
        logger.info("Stop: " + bridge.getLocalAddress() + "---->" + bridge.getRemoteAddress());
    }

    @Override
    public void onOutboundMessage(NetworkBridge bridge, Message message) {

    }

    @Override
    public void onInboundMessage(NetworkBridge bridge, Message message) {

    }

    interface BridgeFailed {
        void processFailed(String key, String url);
    }
}
