package project.bean;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Student {
	private String user_id;
	private String name;
	private String gender;
	private String grade_id;
	private String phone;
	private String email;
	private Date borndate;
	public Student(String user_id, String name, String gender, String grade_id, String phone, String email,
			Date borndate) {
		super();
		this.user_id = user_id;
		this.name = name;
		this.gender = gender;
		this.grade_id = grade_id;
		this.phone = phone;
		this.email = email;
		this.borndate = borndate;
	}
	public Student() {
		super();
	}
	@Override
	public String toString() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy��MM��dd��");
		return user_id + "\t" + name + "\t" + gender + "\t" + grade_id
				+ "\t" + phone + "\t" + email + "\t" + sdf.format(borndate);
	}
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getGrade_id() {
		return grade_id;
	}
	public void setGrade_id(String grade_id) {
		this.grade_id = grade_id;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public Date getBorndate() {
		return borndate;
	}
	public void setBorndate(Date borndate) {
		this.borndate = borndate;
	}
	
}
