package dao;

import bean.Admin;

public interface AdminDAO {
	/**
	 * 根据用户名和密码获取admin数据库中表中的记录
	 *
	 * @param admin
	 * @return Admin：用户名和密码正确 null：用户名或密码不正确
	 */
	public Admin getAdmin(Admin user);

	/**
	 * 根据用户名获取数据库中的记录
	 *
	 * @param admin
	 * @return true：用户名已存在， false：用户名可用
	 */
	public boolean checkUserName(Admin admin);

	/**
	 * 将用户保存到数据库
	 *
	 * @param admin
	 */
	public void saveUser(Admin admin);

}
