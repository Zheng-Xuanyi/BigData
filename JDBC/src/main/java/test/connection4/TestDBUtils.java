package test.connection4;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Test;

import com.atguigu.bean.Admin;
import com.atguigu.utils.JDBCUtils;

/**
 * 此类用于演示DBUtils的使用
 * @author liyuting
 * QueryRunner
 *  	update(connection，sql，params)
 *   	query(connection,sql,ResultSetHandler,params)
 *  ResultSetHandler接口
 *  	实现类：
 *  	BeanHandler 转换成bean
 *  	BeanListHandler  转换成List<bean>
 *  	ScalarHandler 转换成一个Object
 *  	
 *
 */


public class TestDBUtils {
	//测试通用的增删改
	@Test
	public void testUpdate() throws Exception {
		
		QueryRunner qRunner = new QueryRunner();
		Connection connection = JDBCUtils.getConnection();
		
		int update = qRunner.update(connection, "update admin set username=? where id=?", "lily",2);
		System.out.println(update);

	}
	
	//测试通用的查询单条
	@Test
	public void testQuerySingle() throws Exception {
		
		QueryRunner qRunner = new QueryRunner();
		Connection connection = JDBCUtils.getConnection();
				
		//查询
		Admin admin = qRunner.query(connection, "select * from admin where id=?", new BeanHandler<Admin>(Admin.class), 2);
		
		System.out.println(admin);
	}
	//测试通用的查询多条
		@Test
		public void testQueryMulti() throws Exception {			
			QueryRunner qRunner = new QueryRunner();
			Connection connection = JDBCUtils.getConnection();
						
			//查询
			List<Admin> list = qRunner.query(connection, "select * from admin where id<?", new BeanListHandler<Admin>(Admin.class), 10);
			
			for (Admin admin : list) {
				System.out.println(admin);
			}
		}
		
		//测试通用的查询单个值
		@Test
		public void testScalar() throws Exception {			
			QueryRunner qRunner = new QueryRunner();
			Connection connection = JDBCUtils.getConnection();			
			
			//查询
			Object query = qRunner.query(connection, "select count(*) from beauty", new ScalarHandler());		
			
			System.out.println(query);
		}
}
