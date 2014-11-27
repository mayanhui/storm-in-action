<%@page import="storm.javaBean.StormBean"%>
<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>lxy</title>

<style type="text/css">
*{margin:0;padding:0;list-style-type:none;}
a,img{border:0;}
body{font:12px/180% Arial, Helvetica, sans-serif, "新宋体";}
.demo{width:850px;margin:20px auto;}
#l-map{height:600px;width:1000px;float:left;border:1px solid #bcbcbc;}
#r-result{height:100px;width:200px;float:right;}
</style>

<script type="text/javascript" src="http://api.map.baidu.com/api?v=1.4"></script>
<script type="text/javascript" src="http://api.map.baidu.com/api?v=2.0&ak=D7f90c696ca7383e861985a06c7ae3c7"></script>

</head>
<body>
<%
    List<StormBean> s=(List<StormBean>)request.getAttribute("StormList");
    // Double  value_lng =(Double)request.getAttribute("lng");
    // Double  value_lat = (Double)request.getAttribute("lat");
    // String  address = (String)request.getAttribute("address");
%>

<div class="demo">

	<p style="height:40px;">
	获取坐标<input id="lng" type="text"  
	
     <%for(StormBean sto:s){%>
	 
	 value=<%=sto.getLng()%> <%}%>></input>
	<input id="lat" type="text"  <%for(StormBean sto:s){%>  value=<%=sto.getLat()%><%}%>> </input>
	城市：<input id="txtPoint" type="text" <%for(StormBean sto:s){%> value=<%=sto.getAddress()%> <%}%>></input> 
	<input type='button' value='stop' onclick='stop()'>
	</p>
	
	<div id="l-map"></div>
	<div id="r-result"></div>
</div>
<script type="text/javascript">

var map = new BMap.Map("l-map");            // 创建Map实例
	//map.centerAndZoom(new BMap.Point(116.404, 39.915), 1);
	var point =new BMap.Point(-5, 46)
	map.centerAndZoom(point, 1);
	map.enableScrollWheelZoom();
	                    // 将标注添加到地图中
	var local = new BMap.LocalSearch("全球", {
	  renderOptions: {
		map: map,
		panel : "r-result",
		autoViewport: true,
     	selectFirstResult: false
	  }
	});
		//map.addEventListener("click",function(e){		
		//document.getElementById("txtPoint").value=e.point.lng + "," + e.point.lat;
		//document.getElementById("txtPoint").value=156 + "," + 200;
	//});
	//map.getPoint();

function getPoint(){
	 <%for(StormBean sto:s){%>
       // map.clearOverlays();   //清除标记
       // var lng = document.getElementById("lng").value;
       //var lat = document.getElementById("lat").value;	  
     var  value_lng = <%=sto.getLng()%>;
     var  value_lat = <%=sto.getLat()%>;
     <%System.out.println(sto.getLng());%>;
      // var lng = document.value_lng;
      //var lat = document.value_lat;   
	 //window.setInterval("addMap(point1)",1000);
	 //setInterval(" addMap(value_lng,value_lat);",1000);
	 addMap(value_lng,value_lat);
	
	 <%}%>
	//alter("1");
}

 function addMap(value_lng,value_lat){
 	  
      var   point1  = new BMap.Point(value_lng,value_lat); 
      var marker = new BMap.Marker(point1);        // 创建标注    
      map.addOverlay(marker);
     
      //map.clearOverlays(); 
}
function  clean(){
  map.clearOverlays();
 
}
//getPoint();

setInterval("getPoint();",1000);
setInterval("clean();",2000);

//try{Thread.sleep(1000);}catch(Exception e){} 
//window.setInterval("getPoint()",1000);
function stop(){
 //clearInterval(s);
 map.clearOverlays(); 
}
//getPoint();
</script>
<br /><br /><br /><br /><br /><br /><br /><br />	<br /><br /><br /><br />	<br /><br /><br /><br /><br /><br /><br /><br />	<br /><br /><br /><br />
<div style=" width:600px;margin:0 auto; text-align:center; font-size:12px;">

<p></p>
</div>
</body>
</html>