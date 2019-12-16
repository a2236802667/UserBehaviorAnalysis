import java.util.Scanner;

/**
 * Created by Chungjunming on 2019/12/5.
 * 计算字符个数  ABCDEF A
 */
public class Main2 {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String str1 = in.nextLine();
        int n = 0;
        String str2 = in.nextLine();

        String s1 = str1.toLowerCase();
        String s2 = str2.toLowerCase();

        for(int i = 0;i<s1.length();i++){
            if(s2.equals(s1.substring(i,i+1))){
                n = n + 1;
            }
        }
        System.out.println(n);
    }
}
