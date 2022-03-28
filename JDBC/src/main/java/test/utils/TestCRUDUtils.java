package test.utils;

import org.junit.Test;

public class TestCRUDUtils {
	@Test
	public void test() {

		int update = CRUDUtils.update("delete from admin where id=?", 1);
		System.out.println(update);
	}

}
