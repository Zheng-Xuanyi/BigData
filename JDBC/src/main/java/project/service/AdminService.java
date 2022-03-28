package project.service;

import project.bean.Admin;
import project.dao.AdminDAOImpl;

/**
 * 系统管理员Admin相关业务操作
 * @author ZXY
 *
 */
public class AdminService {
	private AdminDAOImpl adminDAOImpl = new AdminDAOImpl();
	/**
	 * 系统管理员登录模块
	 * @param username
	 * @param password
	 */
	public boolean login(String username,String password){
		Admin admin = adminDAOImpl.getAdmin(new Admin(username,password));
		return admin !=null ? true : false;
	}
}
