package project.service;

import project.bean.Admin;
import project.dao.AdminDAOImpl;

/**
 * ϵͳ����ԱAdmin���ҵ�����
 * @author ZXY
 *
 */
public class AdminService {
	private AdminDAOImpl adminDAOImpl = new AdminDAOImpl();
	/**
	 * ϵͳ����Ա��¼ģ��
	 * @param username
	 * @param password
	 */
	public boolean login(String username,String password){
		Admin admin = adminDAOImpl.getAdmin(new Admin(username,password));
		return admin !=null ? true : false;
	}
}
