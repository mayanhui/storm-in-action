<%@page import="org.zkpk.groupb.chart.BarChart1"%>
<%@page import="java.io.PrintWriter"%>
<%@page import="org.zkpk.groupb.chart.BarChart"%>
<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    <base href="<%=basePath%>">
    
    <title>My JSP 'index.jsp' starting page</title>
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="cache-control" content="no-cache">
	<meta http-equiv="expires" content="0">    
	<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
	<meta http-equiv="description" content="This is my page">
	<!--
	<link rel="stylesheet" type="text/css" href="styles.css">
	-->
	
  </head>
  <br/>
  <br/>
  <br/>
  <br/>
  <body>
  	<c:forEach items="${filenames }" var="filename">
    	<img src="${pageContext.request.contextPath }/displaychart?filename=${filename}" width=500 height=300 border=0>
    	<br/>
    	<br/>
  	</c:forEach>
  </body>
</html>
