package project.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSourceFactory;

/**
 * ����ʹ��Druid���ӳ�ʵ�ֻ�ȡ������ر���Դ�Ĺ�����
 * @author ZXY
 *
 */
public class DruidUtils {
	static DataSource datasource;
	
	static {
		try {
			Properties properties = new Properties();
			properties.load(DruidUtils.class.getClassLoader().
					getResourceAsStream("project/druid.properties"));
			datasource = DruidDataSourceFactory.createDataSource(properties);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection(){
		try {
			if(datasource!=null)
				return datasource.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void close(ResultSet set,Statement statement,Connection connection){
		try {
			if (set!=null) {
				set.close();
			}
			if (statement!=null) {
				statement.close();
			}
			if (connection!=null) {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Connection conn = getConnection();
		System.out.println(conn);
		
		String sql = "select * from admin";
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet set = ps.executeQuery();

		while(set.next()){
			String username = set.getString(1);
			String password = set.getString(2);
			System.out.println(username+":"+password);
		}
		
		close(set,ps,conn);
	}
}
