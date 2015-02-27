package com.xin.projects.amq.clustering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;


/**
 * Created by xin on 2015/1/26.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private final static String zkconfigUrl = "conf/zkconfig.properties";

    public static void main(String args[]) throws Exception {
        String zkconn;
        String znode;
        String localName;
        String localUrl;
        String category;
        try {
//            URL prop = Thread.currentThread().getContextClassLoader().getResource(zkconfigUrl);
            Properties properties = new Properties();
//            properties.load(new FileInputStream(prop.getFile()));
            properties.load(new FileInputStream(new File(zkconfigUrl)));
            zkconn = properties.getProperty("zk.conn");
            znode = properties.getProperty("zk.node");
            category = properties.getProperty("zk.category");
            localName = properties.getProperty("amq.name");
            localUrl = properties.getProperty("amq.url");
        } catch (Exception e) {
            logger.error(zkconfigUrl + " file load error!", e);
            throw e;
        }
        if (zkconn == null || znode == null || localName == null || localUrl == null) {
            String message = "Error ! Param cant be null! : zk.conn=" + zkconn + " zk.node=" + znode + " amq.name=" + localName + " amq.url=" + localUrl;
            System.out.println(message);
            throw new Exception(message);
        }
        EmbedBroker embedBroker = new EmbedBroker(localName, localUrl);
        embedBroker.setup();
        new PoolHandler(zkconn, znode, category, localName, localUrl, embedBroker);

    }
}
