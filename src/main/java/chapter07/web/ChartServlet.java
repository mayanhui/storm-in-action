 package chapter07.web;
 
 import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
 
 @SuppressWarnings("serial")
public class ChartServlet extends HttpServlet
 {
   public void doGet(HttpServletRequest request, HttpServletResponse response)
     throws ServletException, IOException
   {
     doPost(request, response);
   }
 
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
     HBaseDao rfh = new HBaseDao();
     rfh.Read();
   }
 }
