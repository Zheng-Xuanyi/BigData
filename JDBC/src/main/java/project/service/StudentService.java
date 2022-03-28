package project.service;

import java.util.List;

import project.bean.Student;
import project.dao.StudentDAOImpl;

/*
 * 1��	ͳ��ѧ������
	2��	�鿴ѧ������
	3��	��ѧ�Ų�ѯѧ������
	4��	��������ѯѧ����Ϣ
	5��	�޸�ѧ����������
	6��	ɾ��ѧ����¼
	7��	�����꼶��¼
	8��	��¼
 */
public class StudentService {
	private StudentDAOImpl sdi = new StudentDAOImpl();
	
	/**
	 * ͳ��ѧ������
	 * @return
	 */
	public long countStudent() {
		return sdi.countStudent();
	}
	
	/**
	 * �鿴ѧ������
	 */
	public List<Student> stuList() {
		return sdi.stuList();
	}
	
	/**
	 * ��ѧ�Ų�ѯѧ������
	 */
	public String getStuName(String user_id) {
		Object value = sdi.getSingleValue("select name from student where user_id = ?", user_id);
		return (String)value;
	}
	
	/**
	 * ��������ѯѧ����Ϣ
	 */
	public List<Student> getStudents(String name) {
		String sql = "select user_id,name,gender,grade_id,phone,email,borndate from student where name like ?";
		List<Student> list = sdi.getBeanList(Student.class, sql, "%"+name+"%");
		return list;
	}
	
	/**
	 * �޸�ѧ����������
	 */
	public boolean updateStuBorn(String user_id,String date) {
		String sql = "update student set borndate = ? where user_id = ?";
		int update = sdi.update(sql, date ,user_id);
		return update>0?true:false;
	}
	
	/**
	 * ɾ��ѧ����¼
	 */
	public boolean deletetStu(String user_id) {
		String sql = "delete from student where user_id = ?";
		int update = sdi.update(sql, user_id);
		return update>0?true:false;
	}
}
