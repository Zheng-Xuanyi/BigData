import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * @author zxy
 * @create 2021-06-12 15:01
 */
public class zkClient {
    public String connStr = "hadoop101:2181,hadoop103:2181,hadoop103:2181";
    public int sessionTimeout = 2000;
    public ZooKeeper zkConn = null;
    public Logger logger = Logger.getLogger(zkClient.class);

    public ZooKeeper getConnection() throws IOException {

        zkConn = new ZooKeeper(connStr, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {

                try {
                    getChildNode();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        return zkConn;
    }

    public void getChildNode() throws KeeperException, InterruptedException {

        List<String> children = zkConn.getChildren("/servers", true);

        logger.info("-----服务器状态：------");
        logger.info(children.toString());

    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        //1.获取zk连接
        zkClient zkClient = new zkClient();
        ZooKeeper zk = zkClient.getConnection();

        //2.获取服务器节点、并设置监听
        zkClient.getChildNode();

        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }
}


