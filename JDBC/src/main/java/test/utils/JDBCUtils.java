package test.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import test.connection2.TestDruidDataSource;

/**
 * 此类用于封装数据库连接的工具类
 * @author liyuting
 * 功能：
 * 1、获取连接
 * 2、释放资源
 *
 */
public class JDBCUtils {
	
	static	DataSource dataSource;
	static{
		try {
			Properties properties = new Properties();
			properties.load(TestDruidDataSource.class.getClassLoader().getResourceAsStream("druid.properties"));
			 dataSource = DruidDataSourceFactory.createDataSource(properties);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 功能：通过Druid数据库连接池获取可用的连接对象
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection() throws Exception{
		return dataSource.getConnection();
	}
	/**
	 * 功能：释放资源
	 * @param set
	 * @param statement
	 * @param connection
	 * @throws Exception
	 */
	
	public static void close(ResultSet set,Statement statement,Connection connection) throws Exception{
		if (set!=null) {
			set.close();
		}
		if (statement!=null) {
			statement.close();
		}
		if (connection!=null) {
			connection.close();
		}
	}
}
