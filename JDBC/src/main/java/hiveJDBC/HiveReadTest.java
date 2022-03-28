package hiveJDBC;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.junit.Test;
import utils.MySQLUtil;
import java.sql.*;

public class HiveReadTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.40.86:10000/temp"
                , "hdfs", "123456");
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("SELECT * from tmp_test_20220323 limit 10");
        while (rs.next()) {
            System.out.println(rs.getString(1) + "," + rs.getString(2));
        }
        rs.close();
        st.close();
        con.close();
    }

    @Test
    public void getHiveData() throws SQLException {
        DruidDataSource dataSource = MySQLUtil.getDataSource("org.apache.hive.jdbc.HiveDriver"
                ,"jdbc:hive2://192.168.40.86:10000/temp"
                , "hdfs", "123456");

        DruidPooledConnection connection = dataSource.getConnection();

        String sql = "SELECT * from tmp_test_20220323 limit 10";
        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()){
            String field1 = resultSet.getString(1);
            String field2 = resultSet.getString(2);

            System.out.println(field1 + "---" +field2 + "---");
        }

        statement.close();
        connection.close();
    }

}
