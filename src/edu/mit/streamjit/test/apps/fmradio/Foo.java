package edu.mit.streamjit.test.apps.fmradio;

import java.io.*;

public class Foo {

   public static void main(String[] args) throws IOException {
      File file = new File("data/fmradio.in");  // change to whatever you want for input.

      int ch;
      StringBuffer strContent = new StringBuffer("");
      // Instead of a string buffer you might want to create an
      //  output file to hold strContent.
      // strContent is probably going to be... messy :-)


      FileInputStream fin = fin = new FileInputStream(file);

      int charCnt = 0;
      int readableCnt = 0;
      int cutoff = -1; // for testing, set to -1 for all input.
      while( (ch = fin.read()) != -1) {
         ++charCnt;
         char readable = '.'; // default to smth for not-so-readable; replace w/your favorite char here.
         // lots of different ways to test this.
         // If your data is relatively simple, you might want to define
         // "readable" as anything from ascii space through newline.
            strContent.append((float)ch).append(" ");
            readable = (char)ch;
            ++readableCnt;
      }
      fin.close();
     // System.out.println("total chars: "+charCnt);
     // System.out.println("readable chars: "+readableCnt);
     // System.out.println("\n--- BEGIN READABLE STUFF---");
      System.out.println(strContent);
     // System.out.println("\n--- END BEGIN READABLE STUFF---");
   }

}