package project.bean;

import java.sql.Date;

public class Grade {
	private String id;
	private String name;
	private Date startDate;
	
	public Grade() {
		super();
	}

	public Grade(String id, String name, Date startDate) {
		super();
		this.id = id;
		this.name = name;
		this.startDate = startDate;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Grade [id=" + id + ", name=" + name + ", startDate=" + startDate + "]";
	}
}
