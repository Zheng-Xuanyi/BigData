package project.service;

import project.dao.GradeDAOImpl;

public class GradeService{
	private GradeDAOImpl gdi = new GradeDAOImpl();
	/**
	 * �����꼶��¼
	 * @return
	 */
	public boolean insertGrade(String id,String name,String date) {
		String sql = "insert into grade values(?,?,?)";
		int update = gdi.update(sql, id,name,date);
		return update>0?true:false;
	}
}
