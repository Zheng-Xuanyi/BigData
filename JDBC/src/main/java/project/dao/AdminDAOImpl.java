package dao;

import bean.Admin;
import project.dao.BasicDAO;

/**
 * 用于封装Admin相关的数据库操作
 * @author ZXY
 *
 */
public class AdminDAOImpl extends BasicDAO implements dao.AdminDAO {

	@Override
	public Admin getAdmin(Admin user) {
		String sql = "select username,password from admin "
				+ "where username = ? and password = ?";
		Admin admin = getBean(user.getClass(),sql, user.getUsername(),user.getPassword());
		return admin;
	}

	@Override
	public boolean checkUserName(Admin admin) {
		String sql = "select count(*) from admin where username = ?";
		Object value = getSingleValue(sql, admin.getUsername());
		if((Integer)value>0)
			return true;
		else
			return false;
	}

	@Override
	public void saveUser(Admin admin) {
		if(!checkUserName(admin)){
			String sql = "insert into admin values(?,?)";
			int update = update(sql, admin.getUsername(),admin.getPassword());
		}
	}

}
