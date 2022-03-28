import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * @author zxy
 * @create 2021-06-12 15:01
 */
public class zkServers {
    public String connStr = "hadoop101:2181,hadoop103:2181,hadoop103:2181";
    public int sessionTimeout = 2000;
    public ZooKeeper zkConn = null;
    public Logger logger = Logger.getLogger(zkServers.class);

    public void getConnection() throws IOException {

        zkConn = new ZooKeeper(connStr, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

//                try {
//                    logger.info("-----新的服务器状态：------");
//                    List<String> children = zkConn.getChildren("/servers", true);
//                    logger.info(children.toString());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
        });

    }

    public void setChildNode(String nodeName) throws KeeperException, InterruptedException {

//        List<String> children = zkConn.getChildren("/servers", true);

        String stat = zkConn.create("/servers/" + nodeName, nodeName.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("-----创建服务器节点：" + stat.toString());

    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        //1.获取zk连接
        zkServers zkClient = new zkServers();
        zkClient.getConnection();

        //2.创建临时、序列的node
        zkClient.setChildNode(args[0]);

        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }
}


