package test.connection3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

import com.atguigu.utils.JDBCUtils;


/**
 * 此类用于演示JDBC中的事务
 * @author liyuting
 * 步骤：
 *  1、	取消自动提交，并开启新事务
     connection.setAutoCommit(false);
	2、	编写事务的组成语句
	3、	结束事务（提交/回滚）
	 connection.commit();
	 connection.rollback();
	 
   案例：小明转账500给杨颖
   注意：一定要保证执行增删改查的connection和开启事务的connection必须是同一个！！！
   
   
 */
public class TestTransaction {
	//测试：不用事务
	@Test
	public void testNoTransaction() throws Exception {

		//1.获取连接
		Connection connection = JDBCUtils.getConnection();
		//2.执行增删改查语句
		
		PreparedStatement statement = connection.prepareStatement("update account set balance = ? where name = ?");
		//①黄晓明少了500
		statement.setDouble(1, 500);
		statement.setString(2, "黄晓明");
		statement.executeUpdate();
		
		
		//模拟异常
		int i = 1/0;
		//②杨颖多了500
		statement.setDouble(1, 1500);
		statement.setString(2, "杨颖");
		statement.executeUpdate();
		
		
		//3.释放资源
		JDBCUtils.close(null, statement, connection);
		
		
	}
	//测试：使用事务
	@Test
	public void testTransaction() {

		Connection connection = null;
		PreparedStatement statement = null;
		try{
			//1.获取连接
			 connection = JDBCUtils.getConnection();
			//--------------------步骤1：开启事务
			connection.setAutoCommit(false);
			//--------------------步骤2：编写事务的组成语句
			
			//2.执行增删改查语句
			
			 statement = connection.prepareStatement("update account set balance = ? where name = ?");
			//①：黄 少了500
			statement.setDouble(1, 500);
			statement.setString(2, "黄晓明");
			
			statement.executeUpdate();
			
			int i=1/0;
			
			//②：杨多了500
			statement.setDouble(1, 1500);
			statement.setString(2, "杨颖");
			
			statement.executeUpdate();
			
			//--------------------步骤3：结束事务
			connection.commit();
		
		}catch(Exception e){
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}finally {
			try {
				//3.释放
				JDBCUtils.close(null, statement, connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
