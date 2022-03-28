package test.connection1;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


/**
 * 此类用于演示JDBC的使用步骤三：执行增删改查
 * @author liyuting

 *
 */
public class TestJDBC_3 {
	
	public static void main(String[] args) throws Exception {
		//步骤1：注册驱动
		Class.forName("com.mysql.jdbc.Driver");
		
		//步骤2：获取连接

		Properties info = new Properties();
		info.load(TestJDBC_3.class.getClassLoader().getResourceAsStream("db.properties"));
		Connection connection = DriverManager.getConnection("jdbc:mysql:///girls", info);
		
		
		//步骤3：执行增删改查
		//①编写sql
//		String sql = "insert into admin values(null,'zsf','0000')";
		String sql = "select id,name bname,sex bsex,borndate bd from beauty";
		
		//②获取执行sql语句的命令对象
		Statement statement = connection.createStatement();
		//③执行sql语句
//		int update = statement.executeUpdate(sql);//执行增删改语句，返回受影响的行数
		
		ResultSet set = statement.executeQuery(sql);//执行查询语句，返回结果集
		
//		boolean execute = statement.execute(sql);//执行任何sql语句，返回是否为结果集
		
		//④处理返回结果
//		System.out.println(update>0?"success":"failure");
		while (set.next()) {
			int id = set.getInt("id");
			String name = set.getString("bname");
			char sex = set.getString("bsex").charAt(0);
			Date date = set.getDate("bd");
			System.out.println(id+"\t"+name+"\t"+sex+"\t"+date);
			
			
		}
		
		
		//步骤4：关闭资源
		set.close();
		statement.close();
		connection.close();
	}

}
