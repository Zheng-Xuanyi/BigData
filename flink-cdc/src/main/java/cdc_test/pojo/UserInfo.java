package cdc_test.pojo;

/**
 * @author zxy
 * @create 2022-03-30 21:54
 */
public class UserInfo {

    private int id;
    private String name;
    private String sex;

    public UserInfo(int id, String name, String sex) {
        this.id = id;
        this.name = name;
        this.sex = sex;
    }

    public UserInfo() {
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                '}';
    }
}

