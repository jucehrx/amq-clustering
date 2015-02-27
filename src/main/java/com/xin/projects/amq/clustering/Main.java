package com.xin.projects.amq.clustering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;


/**
 * Created by xin on 2015/1/26.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private final static String zkconfigUrl = "zkconfig.properties";

    public static void main(String args[]) throws Exception {
        String zkconn;
        String znode;
        String localName;
        String localUrl;
        String category;
        try {
//            URL prop = Thread.currentThread().getContextClassLoader().getResource(zkconfigUrl);
            Properties properties = loadConfig(zkconfigUrl);
//            properties.load(new FileInputStream(prop.getFile()));
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
        final EmbedBroker embedBroker = new EmbedBroker(localName, localUrl);
        embedBroker.setup();
        new PoolHandler(zkconn, znode, category, localName, localUrl, embedBroker);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                embedBroker.stop();
            }
        });

    }

    private static Properties loadConfig(String fileName) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);
            Properties p = new Properties();
            InputStream input = url.openStream();
            if (input != null) {
                try {
                    p.load(input);
                } finally {
                    try {
                        input.close();
                    } catch (Throwable t) {
                    }
                }
            }
            return p;
        } catch (Exception e) {
            logger.warn("Fail to load " + fileName + " file: " + e.getMessage(), e);
        }
        return null;
    }
}
