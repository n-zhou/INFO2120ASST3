import java.util.Date;
import java.sql.Timestamp;

public class Test{

  public static void main(String[] args){
    //Timestamp(int year, int month, int date, int hour, int minute, int second, int nano)
    Date date = new Date("05/21/2020 12:57 PM");
    //Timestamp ts = new Timestamp(date.getYear(), date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds(), 0);
    Timestamp ts = new Timestamp(date.getTime());
    System.out.println(ts.toString());
  }

}
