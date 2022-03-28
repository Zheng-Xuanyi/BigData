package test.connection3;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.junit.Test;

import com.alibaba.druid.util.JdbcUtils;
import com.atguigu.utils.JDBCUtils;

/**
 * 此类用于演示批处理
 * @author liyuting
 * 案例：批量插入admin表50000行记录
 * 相关方法：
 *  addBatch()：添加需要批量处理的SQL语句或参数
	executeBatch()：执行批量处理语句；
	clearBatch():清空批处理包的语句
   批处理+PreparedStatement：大大减少编译次数和执行次数，提高了效率！！！
 */
public class TestBatch {

	//测试：不用批处理
	@Test
	public void testNoBatch() throws Exception {

		//1.获取连接
		Connection connection = JDBCUtils.getConnection();
		
		//2.执行插入
		
		PreparedStatement statement = connection.prepareStatement("insert into admin values(null,?,?)");
		for (int i = 0; i < 50000; i++) {
			statement.setString(1, "john"+i);
			statement.setString(2, "1234");
			statement.executeUpdate();
		}
		//3.释放资源
		JDBCUtils.close(null, statement, connection);
	}
	
	//测试：使用批处理
		@Test
		public void testBatch() throws Exception {

			//1.获取连接
			Connection connection = JDBCUtils.getConnection();
			//2.执行插入
			PreparedStatement statement = connection.prepareStatement("insert into admin values(null,?,?)");
			for (int i = 1; i <= 50000; i++) {
				statement.setString(1, "john"+i);
				statement.setString(2, "1234");
				statement.addBatch();//添加sql语句到批处理包
				if (i%1000==0) {
					statement.executeBatch();//执行批处理包的语句
					statement.clearBatch();//清空批处理包
				}
			}
			//3.释放资源
			JDBCUtils.close(null, statement, connection);
			
		}
}












