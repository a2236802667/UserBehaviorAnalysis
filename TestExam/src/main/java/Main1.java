import java.util.Scanner;

/**
 * Created by Chungjunming on 2019/12/5.
 */
//字符串最后一个单词的长度，单词以空格隔开。
public class Main1 {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String word = in.nextLine();
        int n = word.lastIndexOf(" ");
        if(n == -1){
            System.out.println(word.length());
        }else {
            String str = word.substring(n,word.length() - 1);
            System.out.println(str.length());
        }
    }

}
