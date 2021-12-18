

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestMysql {

    public static void main(String[] args) throws Exception {
        Connection cn=null;
        Statement stmt=null;
        ResultSet rs=null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
//            cn = DriverManager.getConnection("jdbc:mysql://172.16.27.11:3306", "ops", "ops@2020");
            cn = DriverManager.getConnection("jdbc:mysql://10.198.110.63:3306?verifyServerCertificate=false&useSSL=false", "ops", "ops@2020");
            stmt = cn.createStatement();
            rs = stmt.executeQuery("select * from db_user_mng.businesses limit 10");
            while(rs.next()){
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.print(rs.getString(i));
                    System.out.print(",\t");
                }
                System.out.println();
            }
        }finally {
            if(rs!=null)rs.close();
            if(stmt!=null)stmt.close();
            if(cn!=null)cn.close();
        }
    }

}
