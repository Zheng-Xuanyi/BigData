package utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zxy
 */
public class MySQLUtil {
    public static void main(String[] args) throws SQLException {
      DruidDataSource dataSource = getDataSource("com.mysql.jdbc.Driver"
                                                  ,"jdbc:mysql://192.168.40.12:3306/bdpis"
                                                  , "root", "123456");

      DruidPooledConnection connection = dataSource.getConnection();

      String sql = "select * from shop_week_avg_finish_cnt where id = 1";
      Statement statement = connection.createStatement();

      ResultSet resultSet = statement.executeQuery(sql);

      while(resultSet.next()){
          String field1 = resultSet.getString(1);
          String field2 = resultSet.getString(2);
          String field3 = resultSet.getString(3);

          System.out.println(field1 + "---" +field2 + "---" +field3 + "---" );
      }

      statement.close();
      connection.close();

  }

    private static final Logger LOG = LoggerFactory.getLogger("MySQLUtil");
  
    public static DruidDataSource getDataSource(String driverClassName,
                                              String dbUrl,
                                              String user,
                                              String password) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(dbUrl);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setMinIdle(1);
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(2);
        dataSource.setValidationQuery("select 1");
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnReturn(false);
        dataSource.setTestWhileIdle(true);
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(300 * 2);
        try {
            dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return dataSource;
    }
  
  
}
