package test.connection1;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


/**
 * 此类用于演示JDBC的使用步骤二：获取连接
 * @author liyuting

 *
 */
public class TestJDBC_2 {
	
	public static void main(String[] args) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		//方式1：
//		Connection connection = DriverManager.getConnection("jdbc:mysql:///girls", "root", "root");
		
		//方式2：
//		Connection connection = DriverManager.getConnection("jdbc:mysql:///girls?user=root&password=root");
		
		
		//方式3：
		Properties info = new Properties();
		info.load(TestJDBC_2.class.getClassLoader().getResourceAsStream("db.properties"));
		Connection connection = DriverManager.getConnection("jdbc:mysql:///girls", info);
		
		
		System.out.println(connection);
	}

}
