package dao;

import java.util.List;

import bean.Student;

public class StudentDAOImpl extends dao.BasicDAO {


	/**
	 * 统计学生人数
	 * @return
	 */
	public long countStudent() {
		Object obj = getSingleValue("select count(*) from student");
		return (Long)obj;
	}

	/**
	 * 返回学生名单
	 * @return
	 */
	public List<Student> stuList() {
		String sql = "select user_id,name,gender,grade_id,phone,email,borndate from student where user_id is not null";
		List<Student> list = getBeanList(Student.class, sql);
		return list;
	}

}
