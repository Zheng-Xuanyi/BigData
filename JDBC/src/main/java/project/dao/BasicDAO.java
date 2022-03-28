package dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import utils.DruidUtils;

public class BasicDAO {
	private QueryRunner qr = new QueryRunner();
	/**
	 * 执行增删改操作
	 * @param sql
	 * @param objects
	 * @return
	 */
	public int update(String sql,Object...objects) {
		Connection conn = DruidUtils.getConnection();
		int count;
		try {
			count = qr.update(conn, sql,objects);
			return count;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(null, null, conn);
		}
	}
	//查询单行
	public <T> T getBean(Class<T> t,String sql,Object...objs) {

		Connection conn = DruidUtils.getConnection();
		T query;
		try {
			query = qr.query(conn, sql,new BeanHandler<T>(t), objs);
			return query;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(null, null, conn);
		}
	}

	//查询多行
	public <T> List<T> getBeanList(Class<T> t,String sql,Object...objs) {

		Connection conn = DruidUtils.getConnection();
		List<T> list;
		try {
			list = qr.query(conn, sql,new BeanListHandler<T>(t), objs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(null, null, conn);
		}
		return list;
	}
	//查询单一值
	public Object getSingleValue(String sql, Object... objs) {
		// 获取连接
		Connection conn = DruidUtils.getConnection();
		Object value = null;
		try {
			value = qr.query(conn, sql, new ScalarHandler(), objs);
			return value;
		} catch (SQLException e) {
			//将编译时异常转换为运行时异常向上抛
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(null, null, conn);
		}
	}

	//批处理查询
	public int[] batchQuery(String sql, Object[][]... objs) {
		// 获取连接
		Connection conn = DruidUtils.getConnection();
		int[] batch = null;
		try {
			batch = qr.batch(conn, sql, objs);
			return batch;
		} catch (SQLException e) {
			//将编译时异常转换为运行时异常向上抛
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(null, null, conn);
		}
	}
}
