package com.xin.projects.amq.clustering;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by xin on 2015/1/26.
 */
public class PoolHandler extends Thread implements Watcher, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {
    private static final Logger logger = LoggerFactory.getLogger(EmbedBroker.class);
    private final static String zkconfigUrl = "conf/zkconfig.properties";
    private final static String DefaultZkconn = "127.0.0.1:2181";
    private final static String DefaultZnode = "/amq-clustering";
    private String zkconn;
    private String znode;
    private String localName;
    private String localUrl;
    private ZooKeeper zk;
    private PoolListener listener;
    private boolean dead = false;

    public PoolHandler(String brokerName, String brokerUrl, PoolListener listener) throws Exception {
        this.listener = listener;
        if (brokerName == null || brokerUrl == null) {
            String error = "broker name and broker url cant be null!!" + brokerName + "||" + brokerUrl;
            logger.error(error);
            throw new Exception(error);
        }
        this.localName = brokerName;
        this.localUrl = brokerUrl;
        try {
            URL prop = Thread.currentThread().getContextClassLoader().getResource(zkconfigUrl);
            Properties properties = new Properties();
            properties.load(new FileInputStream(prop.getFile()));
            String conn = properties.getProperty("zk.conn");
            String node = properties.getProperty("zk.node");
            this.zkconn = conn == null ? DefaultZkconn : conn;
            this.znode = node == null ? DefaultZnode : node;
        } catch (Exception e) {
            logger.error(zkconfigUrl + " file load error!", e);
            throw e;
        }
        zk = new ZooKeeper(zkconn, 3000, this);
        zk.exists(znode, true, this, znode);
        start();
    }

    public interface PoolListener {
        void changes(Map<String, String> urls);
    }


    @Override
    public void run() {
        try {
            synchronized (this) {
                while (!dead) {
                    wait();
                }
                if (dead) {
                    new PoolHandler(localName, localUrl, listener);
                    throw new Exception("connect dead! lost connect with zk!");
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
                case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                case Expired:
                    // It's all over
                    dead = true;
                    break;
            }
        } else {
            if (path != null && path.equals(znode)) {
                zk.getChildren(znode, true, this, null);
            }
        }

    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        boolean exists;

        switch (rc) {
            case Code.Ok:
                exists = true;
                break;
            case Code.NoNode:
                exists = false;
                break;
            case Code.SessionExpired:
            case Code.NoAuth:
                dead = true;
                notifyAll();
                return;
            default:
                // Retry errors
                zk.getChildren(znode, true, this, null);
                return;
        }


        if (exists && listener != null) {
            listener.changes(getData(children));
        } else {

        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;
        switch (rc) {
            case Code.Ok:
                exists = true;
                break;
            case Code.NoNode:
                exists = false;
                break;
            case Code.SessionExpired:
            case Code.NoAuth:
                dead = true;
                notifyAll();
                return;
            default:
                zk.exists(znode, true, this, null);
                return;
        }
        if (path.equals(znode)) {
            try {
                if (exists) {
                    zk.create(znode + "/" + localName, localUrl.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    zk.getChildren(znode, true, this, null);
                } else {
                    zk.create(znode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    zk.create(znode + "/" + localName, localUrl.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    zk.getChildren(znode, true, this, null);
                }

            } catch (Exception e) {
                logger.error(e.getMessage());
                System.out.println(e.getMessage());
            }
        }
    }

    private Map<String, String> getData(List<String> list) {
        HashMap<String, String> map = new HashMap<String, String>();
        for (String path : list) {
            try {
                byte[] b = zk.getData(znode + "/" + path, false, null);
                map.put(path, new String(b));
            } catch (Exception e) {
                logger.debug(e.getMessage());
                System.out.println(e.getMessage());
            }
        }
        return map;
    }


}
