package dao;

import bean.Admin;

public interface AdminDAO {
	/**
	 * �����û����������ȡadmin���ݿ��б��еļ�¼
	 *
	 * @param admin
	 * @return Admin���û�����������ȷ null���û��������벻��ȷ
	 */
	public Admin getAdmin(Admin user);

	/**
	 * �����û�����ȡ���ݿ��еļ�¼
	 *
	 * @param admin
	 * @return true���û����Ѵ��ڣ� false���û�������
	 */
	public boolean checkUserName(Admin admin);

	/**
	 * ���û����浽���ݿ�
	 *
	 * @param admin
	 */
	public void saveUser(Admin admin);

}
