package project.view;

import java.util.List;
import java.util.Scanner;

import project.bean.Student;
import project.service.AdminService;
import project.service.GradeService;
import project.service.StudentService;
import service.*;
import project.utils.TSUtility;

public class Views {
	Scanner input = new Scanner(System.in);
	AdminService service = new AdminService();
	StudentService stuService = new StudentService();
	GradeService gradeService = new GradeService();

	/**
	 * ϵͳ����Ա��¼
	 * 
	 * @throws Exception
	 */
	public boolean login() throws Exception {
		System.out.print("�������û�����");
		String username = input.next();
		System.out.print("���������룺");
		String password = input.next();
		if (service.login(username.trim(), password)) {
			System.out.println("��¼�ɹ���");
			return true;
		} else {
			System.out.println("��¼ʧ�ܣ�");
			return false;
		}
	}

	/**
	 * ͳ��ѧ������
	 * 
	 * @return
	 */
	public void countStu() {
		System.out.println("����ѧ�� " + stuService.countStudent()+" ��");
	}

	/**
	 * 2��	�鿴ѧ������
	 */
	public void stuList() {
		System.out.println("-------------------------------ѧ���б�-----------------------------");
		System.out.println("ѧ��\t����\t�Ա�\t�꼶\t�绰\t\t����\t��������");
		
		List<Student> list = stuService.stuList();
		for (Student student : list) {
			System.out.println(student);
		}
	}
	
	public void getStuName() {
		System.out.print("������ѧ��ѧ�ţ�");
		String user_id = input.next();
		String stuName = stuService.getStuName(user_id);
		System.out.println(stuName);
	}
	
	public void getStuInfo() {
		System.out.print("������ѧ��������");
		String name = input.next();
		List<Student> students = stuService.getStudents(name);
		System.out.println("-------------------------------ѧ���б�-----------------------------");
		System.out.println("ѧ��\t����\t�Ա�\t�꼶\t�绰\t\t����\t��������");
		
		for (Student student : students) {
			System.out.println(student);
		}
	}
	
	public void updateStuBorn() {
		System.out.print("������ѧ��ѧ�ţ�");
		String user_id = input.next();
		System.out.print("������ѧ���������ڣ�");
		String date = input.next();
		boolean born = stuService.updateStuBorn(user_id, date);
		if(born)
			System.out.println("���³ɹ���");
		else
			System.out.println("����ʧ�ܣ�");
	}
	
	public void deleteStu() {
		System.out.print("������Ҫɾ����ѧ��ѧ�ţ�");
		String user_id = input.next();
		boolean b = stuService.deletetStu(user_id);
		if(b)
			System.out.println("ɾ���ɹ���");
		else
			System.out.println("ɾ��ʧ�ܣ�");
	}
	
	public void insertGrade() {
		System.out.print("�������꼶�ţ�");
		String id = input.next();
		
		System.out.print("�������꼶���ƣ�");
		String name = input.next();
		
		System.out.print("�������꼶���");
		String date = input.next();
		
		boolean b = gradeService.insertGrade(id, name, date);
		
		if(b)
			System.out.println("����ɹ���");
		else
			System.out.println("����ʧ�ܣ�");
	}
	
	public void menu() {
		System.out.println("===================��ѡ�������================");
		System.out.println("1��	ͳ��ѧ������");
		System.out.println("2��	�鿴ѧ������");
		System.out.println("3��	��ѧ�Ų�ѯѧ������");
		System.out.println("4��	��������ѯѧ����Ϣ");
		System.out.println("5��	�޸�ѧ����������");
		System.out.println("6��	ɾ��ѧ����¼");
		System.out.println("7��	�����꼶��¼");
		System.out.println("0��	�˳�");
	}
	
	public void appMenu() {
		menu();
		char index;
		while(true){
			index = TSUtility.readMenuSelection();
//			switch index
			switch (index) {
			case '0':
				System.out.print("�Ƿ��˳�ϵͳ��Y/N");
				if(TSUtility.readConfirmSelection()=='Y')
					return;
				else
					break;
			case '1':
				countStu();
				break;
			case '2':
				stuList();
				break;
			default:
				break;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Views v = new Views();
		if(v.login()){
			v.appMenu();
		}
	}
}
