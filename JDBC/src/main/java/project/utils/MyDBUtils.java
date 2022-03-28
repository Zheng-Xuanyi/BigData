package project.utils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import project.bean.Admin;

/**
 * ���ܣ���װͨ�õ���ɾ�Ĳ鷽�� 
 * 1.ͨ�õ���ɾ�ģ�����κα��κ���ɾ����� 
 * 2.ͨ�õĲ�ѯ1����Ե������κβ�ѯ����
 * 3.ͨ�õĲ�ѯ2����Ե������κβ�ѯ����
 * 
 * @author ZXY
 *
 */
public class MyDBUtils {
	/**
	 * 1.ͨ�õ���ɾ�ģ�����κα��κ���ɾ����� 
	 * @throws Exception
	 * 
	 */
	public static int update(String sql, Object... objects) {
		Connection conn = null;
		PreparedStatement ps = null;

		try {
			conn = DruidUtils.getConnection();
			ps = conn.prepareStatement(sql);
			
			for (int i =0;i<objects.length;i++) {
				ps.setObject(i+1, objects[i]);
			}
			
			return ps.executeUpdate();
		} catch (Exception e) {
			// �������쳣ת���������쳣
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(null, ps, conn);
		}
	}
	
	/**
	 * 2.ͨ�õĲ�ѯ1����Ե������κβ�ѯ����
	 */
	public static<T> T selectSingle(String sql,Class<T> t,Object...objects) {
		Connection conn = DruidUtils.getConnection();
		PreparedStatement statement = null;
		ResultSet set = null;
		T result = null;
		try{
			statement = conn.prepareStatement(sql);
			for(int i=0;i<objects.length;i++){
				statement.setObject(i+1, objects[i]);
			}
			
			set = statement.executeQuery();
			
			while(set.next()){
				result = t.newInstance();
				Field[] fields = t.getDeclaredFields();
				for (int i =0;i<fields.length;i++) {
					fields[i].setAccessible(true);
					fields[i].set(result, set.getObject(i+1));
				}
				return result;
			}
			return null;
		}catch (Exception e){
			throw new RuntimeException(e);
		} finally {
			DruidUtils.close(set,statement, conn);
		}
	}
	
	public static void main(String[] args) {
//		int i = update("delete from admin where username = ?","user1");
//		System.out.println(i);
		
		Admin admin = selectSingle("select * from admin where username = ?", Admin.class ,"user2");
		System.out.println(admin);
	}
}
