package test.connection2;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

/**
 * 此类用于演示Druid连接池的使用
 * @author liyuting
 *
 */
public class TestDruidDataSource {
	public static void main(String[] args) throws Exception {
		
		
		//1.创建Druid连接池对象
		//方式1：通过修改配置文件设置参数【推荐使用】
		Properties properties = new Properties();
		properties.load(TestDruidDataSource.class.getClassLoader().getResourceAsStream("druid.properties"));
		DataSource dataSource = DruidDataSourceFactory.createDataSource(properties);
		
		//方式2：手动调用API设置参数
//		DruidDataSource dataSource = new DruidDataSource();
//		dataSource.setUsername("root");
//		dataSource.setPassword("root");
//		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//		//...
//		
//		dataSource.setInitialSize(10);
		
		
		
		
		//2.获取连接
		Connection connection = dataSource.getConnection();
		System.out.println(connection);
		
		
		//3.执行增删改查
		
		
		//4.释放连接
		
		connection.close();
		
		
	}

}
