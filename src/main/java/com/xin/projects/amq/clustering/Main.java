package com.xin.projects.amq.clustering;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFactory;

/**
 * Created by xin on 2015/1/26.
 */
public class Main {
    static String localName = "broker1";
    static String localUrl = "tcp://172.24.127.2:61616";

    public static void main(String args[]) throws Exception {
        EmbedBroker embedBroker = new EmbedBroker(localName, localUrl);
        embedBroker.setup();
        String localName = embedBroker.getBroker().getBrokerName();
        String localUrl = embedBroker.getBroker().getDefaultSocketURIString();

        new PoolHandler(localName, localUrl, embedBroker);
    }
}
