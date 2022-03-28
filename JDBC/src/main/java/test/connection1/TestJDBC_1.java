package test.connection1;

import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * 此类用于演示JDBC的使用步骤一：注册驱动
 * @author liyuting
 * 不足：
 * 1、导致Driver new 了两遍，效率较低
 * 2、编译期加载，类的依赖性太高
 *
 */
public class TestJDBC_1 {
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
//		DriverManager.registerDriver(new Driver());
		Class.forName("com.mysql.jdbc.Driver");
	}

}
