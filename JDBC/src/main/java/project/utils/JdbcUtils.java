package project.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
/**
 * ����ʹ�û�����JDBC API������ʵ�����ݿ�������رղ����Ĺ�����
 * @author ZXY
 *
 */
public class JdbcUtils {
	static Connection connection = null;
	
	static{
		try {
			//1.����ע������
			Class.forName("com.mysql.jdbc.Driver");
			//2.��ȡ���ݿ�����
			Properties pro = new Properties();
			pro.load(JdbcUtils.class.getClassLoader().getResourceAsStream("project/Jdbc.properties"));
			connection = DriverManager.getConnection("jdbc:mysql:///student_manager_system",pro);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private JdbcUtils(){}
	
	public static Connection getConnection() {
		//��������
		return connection;
	}
	
	public static void close(ResultSet set, Statement statement ,Connection connection) {
		try {
			if(set!=null)
				set.close();
			if(statement!=null)
				statement.close();
			if(connection!=null)
				connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//�Ըù�������в���
	public static void main(String[] args) throws Exception {
		Connection conn = JdbcUtils.getConnection();
		System.out.println(conn);
		
		String sql = "insert into admin values(?,?)";
		PreparedStatement ps = conn.prepareStatement(sql);
		
		ps.setString(1, "user2");
		ps.setString(2, "123456");
		
		int update = ps.executeUpdate();
		System.out.println(update);
		
		JdbcUtils.close(null,ps,conn);
	}
}
