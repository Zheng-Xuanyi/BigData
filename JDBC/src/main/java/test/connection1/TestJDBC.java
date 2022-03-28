package test.connection1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.Driver;

/**
 * 此类用于演示JDBC的使用步骤
 * @author liyuting
*  准备工作：将mysql-connector-java-5.1.37-bin.jar复制到项目的libs文件夹中
                  并右击 Build Path——>Add to Build Path
        
*	①注册驱动
	
	②获取连接
	
	③执行增删改查
	
	④释放资源
 */
public class TestJDBC {
	public static void main(String[] args) throws SQLException {
//		①注册驱动
		DriverManager.registerDriver(new Driver());
		
//		②获取连接
		
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/girls", "root", "root");
		
		
//		③执行增删改查
		
		String sql = "update beauty set name = '杨小智' where id = 1";
		//创建一个执行sql语句的命令对象
		Statement statement = connection.createStatement();
		//真正执行sql语句，并接受返回值
		int update = statement.executeUpdate(sql);
		//处理返回值
		System.out.println(update>0?"success":"failure");
		
//		④释放资源
		statement.close();
		connection.close();
		
	}

}
