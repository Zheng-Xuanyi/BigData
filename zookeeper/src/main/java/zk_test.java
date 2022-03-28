import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author zxy
 * @create 2021-06-11 8:32
 */
public class zk_test {

    private static String conn = "hadoop101:2181,hadoop102:2181,hadoop103:2181";

    private static int sessionTimeout = 2000;

    private ZooKeeper zkClient = null;

    @Test
    public void init() throws Exception{

        zkClient = new ZooKeeper(conn, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println("收到事件通知后的回调函数:" + watchedEvent.getType() + "--" + watchedEvent.getPath());

                // 再次启动监听
                try {
                    List<String> children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("------------end---------------");
            }
        });

    }

    // 创建子节点
    @Test
    public void create() throws Exception {
        System.out.println("==========");
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String nodeCreated = zkClient.create("/zxy1", "jinlian".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("nodeCreated======" + nodeCreated);

        Stat stat = zkClient.exists("/zxy1", true);


        System.out.println("stat======" + stat);

        System.in.read();

    }


    // 获取子节点
    @Test
    public void getChildren() throws Exception {

        System.out.println("------------start---------------");

        zkClient = new ZooKeeper(conn, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {

                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println("收到事件通知后的回调函数:" + watchedEvent.getType() + "--" + watchedEvent.getPath());

                // 再次启动监听
                try {
                    List<String> children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println("Watcher==========" + child);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("------------end---------------");
            }
        });


        List<String> children = zkClient.getChildren("/", false);

        for (String child : children) {
            System.out.println(child);
        }

        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }
}

