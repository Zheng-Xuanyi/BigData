package test.connection2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner;

import javax.sql.StatementEvent;

import org.junit.Test;

/**
 * 此类用于演示PreparedStatement和Statement的区别
 * @author liyuting
 * 案例：登录案例
 * 
 * 
 * 使用PreparedStatement的好处：
 * 1、有效的避免了sql注入问题
 * 2、不再使用+拼接sql语句，减少了语法错误，提高了语句的简洁性和分离性
 * 3、减少了编译次数，提高了效率
 * 4、可以实现批处理功能
 * 
 */
public class TestPreparedStatement {
	Scanner input = new Scanner(System.in);
	
	//演示Statement
	@Test
	public void testStatement() throws Exception {
		System.out.print("Please input username:");
		String username = input.next();
		System.out.print("Please input password:");
		String password = input.next();
		
		
		//-----------------------以下为访问数据库的步骤------------------------------
		
		//1.注册驱动
		Class.forName("com.mysql.jdbc.Driver");
		
		
		//2.获取连接
		Connection connection = DriverManager.getConnection("jdbc:mysql:///girls", "root", "root");
		
		//3.执行增删改查
		//wer!@#$%^&*"'
		//String sql = "SELECT COUNT(*) FROM admin WHERE username = '"wer!@#$%^&*"'"' AND PASSWORD='"+password+"'";
		String sql = "SELECT COUNT(*) FROM admin WHERE username = '"+username+"' AND PASSWORD='"+password+"'";
		
		Statement statement = connection.createStatement();
		ResultSet set = statement.executeQuery(sql);
		if(set.next()){
			int count = set.getInt(1);
			System.out.println(count>0?"Login Success!":"Login Failure!");
		}else{
			System.out.println("Login Failure!");
		}
		//4.释放资源
		set.close();
		statement.close();
		connection.close();
		

	}
	//演示PreparedStatement
	@Test
	public void testPreparedStatement() throws Exception {
		System.out.print("Please input username:");
		String username = input.next();
		System.out.print("Please input password:");
		String password = input.next();
		
		
		//-----------------------以下为访问数据库的步骤------------------------------
		
		//1.注册驱动
		Class.forName("com.mysql.jdbc.Driver");
		
		
		//2.获取连接
		Connection connection = DriverManager.getConnection("jdbc:mysql:///girls", "root", "root");
		
		//3.执行增删改查
		String sql="select count(*) from admin where username= ? and password = ?";
		
		PreparedStatement statement = connection.prepareStatement(sql);//对sql语句进行了预编译
		
		//设置占位符的值
		statement.setString(1, username);
		statement.setString(2, password);
		
		//执行sql命令
		
//		boolean execute = statement.execute();//执行任何sql语句，返回是否有结果
//		int update = statement.executeUpdate();//执行增删改语句，返回受影响的行数
		ResultSet set = statement.executeQuery();//执行查询语句，返回结果集
		
		//处理结果集
		
		if(set.next()){
			int count = set.getInt(1);
			System.out.println(count>0?"Login Success!":"Login Failure!");
		}else{
			System.out.println("Login Failure!");
		}
		//4.释放资源
		set.close();
		statement.close();
		connection.close();
		
		

	}

}
