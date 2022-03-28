/*1.创建数据库*/
CREATE DATABASE IF NOT EXISTS student_manager_system;

/*2.创建对应表*/

#1)Admin管理员表
DROP TABLE IF EXISTS admin;
CREATE TABLE IF NOT EXISTS admin (
	usename VARCHAR(10) PRIMARY KEY,
	PASSWORD VARCHAR(10) NOT NULL
);

INSERT INTO admin VALUE('root','123456');

# 2)Grade年级表

CREATE TABLE IF NOT EXISTS Grade (
	id VARCHAR(10) PRIMARY KEY,
	NAME VARCHAR(10) ,
	DATE DATE
);


# 3)Student学生表
CREATE TABLE IF NOT EXISTS student (
	user_id VARCHAR(10) PRIMARY KEY, /*学号*/
	NAME VARCHAR(20) NOT NULL,	/*姓名*/
	gender CHAR  DEFAULT '男',
	grade_id VARCHAR(10),
	phone VARCHAR(15),
	email VARCHAR(20),
	borndate DATETIME,
	
	CONSTRAINT fk FOREIGN KEY(grade_id) REFERENCES grade(id)
);
