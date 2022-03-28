package test.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import test.bean.Admin;
import com.mysql.fabric.xmlrpc.base.Array;

/**
 * 功能：封装通用的增删改查方法
		1.通用的增删改：针对任何表任何增删改语句 
		2.通用的查询1：针对单个表任何查询单条 
		3.通用的查询2：针对单个表任何查询多条
 * 
 * @author liyuting
 * 
 *
 */
public class CRUDUtils {

	/**
	 * 功能：封装通用的增删改
	 * 
	 * @param sql
	 *            待执行的sql语句
	 * @param objects
	 *            占位符的值
	 * @return 受影响 的行数
	 * @throws Exception
	 */
	public static int update(String sql, Object... objects) {

		// 1.获取连接
		Connection connection = null;
		// 2.执行sql
		PreparedStatement statement = null;
		try {
			connection = JDBCUtils.getConnection();

			statement = connection.prepareStatement(sql);
			for (int i = 0; i < objects.length; i++) {
				statement.setObject(i + 1, objects[i]);
			}
			return statement.executeUpdate();
			
			
		} catch (Exception e) {
			throw new RuntimeException(e);// 将编译异常转换成运行异常
		} finally {
			try {
				// 3.释放资源
				JDBCUtils.close(null, statement, connection);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}
	/**
	 * 功能:封装通用的查询单条Admin对象
	 * @param sql
	 * @param objects
	 * @return
	 */
	public static Admin querySingle(String sql,Object...objects){
		// 1.获取连接
		Connection connection = null;
		// 2.执行sql
		PreparedStatement statement = null;
		try {
			connection = JDBCUtils.getConnection();

			statement = connection.prepareStatement(sql);
			for (int i = 0; i < objects.length; i++) {
				statement.setObject(i + 1, objects[i]);
			}
			
			ResultSet set = statement.executeQuery();
			if (set.next()) {				
				int id = set.getInt(1);
				String username = set.getString(2);
				String password = set.getString(3);
				return new Admin(id, username, password);
			}
			return null;
		} catch (Exception e) {
			throw new RuntimeException(e);// 将编译异常转换成运行异常
		} finally {
			try {
				// 3.释放资源
				JDBCUtils.close(null, statement, connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 功能:封装通用的查询多条Admin对象
	 * @param sql
	 * @param objects
	 * @return
	 */
	public static List<Admin> queryMulti(String sql,Object...objects){
		// 1.获取连接
		Connection connection = null;
		// 2.执行sql
		PreparedStatement statement = null;
		try {
			connection = JDBCUtils.getConnection();

			statement = connection.prepareStatement(sql);
			for (int i = 0; i < objects.length; i++) {
				statement.setObject(i + 1, objects[i]);
			}
			
			
			ResultSet set = statement.executeQuery();
			List<Admin> list  = new ArrayList<>();
			while (set.next()) {				
				int id = set.getInt(1);
				String username = set.getString(2);
				String password = set.getString(3);
				list.add(new Admin(id, username, password));
			}
			return list;	
			
		} catch (Exception e) {
			throw new RuntimeException(e);// 将编译异常转换成运行异常
		} finally {
			try {
				// 3.释放资源
				JDBCUtils.close(null, statement, connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
