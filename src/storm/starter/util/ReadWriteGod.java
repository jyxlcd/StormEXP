package storm.starter.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class ReadWriteGod {
	public static BufferedReader reader(String path) throws IOException{
        return new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
    }
    
    public static PrintWriter writer(String path) throws IOException{
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8")));
    }
    
    public static PrintWriter writer(String path, boolean append) throws IOException{
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path,append),"UTF-8")));
    }
}
