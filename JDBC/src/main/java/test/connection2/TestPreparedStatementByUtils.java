package test.connection2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner;

import javax.sql.StatementEvent;

import org.junit.Test;

import com.atguigu.utils.JDBCUtils;

/**
 * 此类用于演示通过调用JDBCUtils实现登录
 * @author liyuting
 * 案例：登录案例
 * 

 * 
 */
public class TestPreparedStatementByUtils {
	Scanner input = new Scanner(System.in);
	
	
	//演示PreparedStatement
	@Test
	public void testPreparedStatement() throws Exception {
		System.out.print("Please input username:");
		String username = input.next();
		System.out.print("Please input password:");
		String password = input.next();
		
		
		//-----------------------以下为访问数据库的步骤------------------------------
		
		//1.获取连接
		Connection connection = JDBCUtils.getConnection();
		
		//2.执行增删改查
		String sql="select count(*) from admin where username= ? and password = ?";
		
		PreparedStatement statement = connection.prepareStatement(sql);//对sql语句进行了预编译
		
		//设置占位符的值
		statement.setString(1, username);
		statement.setString(2, password);
		
		//执行sql命令
		ResultSet set = statement.executeQuery();//执行查询语句，返回结果集
		
		//处理结果集
		
		if(set.next()){
			int count = set.getInt(1);
			System.out.println(count>0?"Login Success!":"Login Failure!");
		}else{
			System.out.println("Login Failure!");
		}
		//3.释放资源
		JDBCUtils.close(set, statement, connection);
		
		

	}

}
