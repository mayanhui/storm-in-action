<%@ page language="java" import="java.util.*,com.cyt.model.*" pageEncoding="UTF-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta charset="utf-8">
<title>实时路况</title>
<link rel="stylesheet" type="text/css" href="images/main.css" />
<style type="text/css">
.demo{width:760px; height:500px; margin:30px auto 0 auto; font-size:14px;}
.demo p{line-height:30px}
#map{width:600px; margin:10px auto}
#label{width:200px; height:25px; line-height:25px; margin:10px auto}
#label span{height:25px; width:25px; display:block; float:left; text-align:center}
</style>
<script type="text/javascript" src="js/jquery.js"></script>
<script type="text/javascript" src="js/raphael.js"></script>
<script type="text/javascript" src="js/chinamapPath.js"></script>
</head>

<body>


 <% 
    List<Car> l = (List)request.getAttribute("st");
    		for(Object o: l){
 		Car c=(Car)o;
  		out.println("<tr>");
  			out.println("<td>" +"区域："+c.getDirectId() + "</td>");
   		out.println("<td>" + "车辆个数："+c.getCount() + "</td>");
   		out.print("<a href='javascript:create("+c.getDirectId()+","+c.getCount()+")'>刷新</a>&nbsp;&nbsp;");
   			out.print("<td>");
				out.print("</td>");
   		out.println("</tr>");  		
  		}
  %>

<div id="main">
   <h2 class="top_title" align="center"><a href="#">实时路况</a></h2>
   <div class="demo">
  <div id="label" align="center">
        	<span>多</span>
            <span style="background-color:#e55c86"></span>
            <span style="background-color:#ff84be"></span>
            <span style="background-color:#ffa2e9"></span>
            <span style="background-color:#ffbeef"></span>
            <span style="background-color:#f7d6f5"></span>
            <span style="background-color:#f7eef8"></span>
            <span>少</span>
        </div>
        
       
   		
   		<div id="map"></div>
   		
   </div>
	
</div>


<script type="text/javascript">




window.onload = function() {
   var arr=new Array();
   var flag;
   <%for(Car list:l){%>
   	var count1 = <%=list.getCount()%>
   	var id = <%=list.getDirectId()%>
   	
 
    if(count1<20){
       flag=0;
    }else if(count1>=20&&count1<50){            
   		flag=1; 
    }else if(count1>=50&&count1<70){
   		flag=2;
    }else if(count1>=70&&count1<100){
   		flag=3;
    }else if(count1>=100&&count1<150){
   		flag=4;
    }else{
   		flag=5;
   }
   arr.push(flag);

   <%}%>
   
   var colors=["#f7eef8","#f7d6f5","ffbeef","f0a2e9","#f084be","#f05c86"]; 
    
    
    
    var R = Raphael("map", 1000,1000);
	//调用绘制地图方法
    paintMap(R);
//	var rect = paper.rect(10, 10, 50, 30).attr("fill": "#000");
	var textAttr = {
        "fill": "#000",
        "font-size": "12px",
        "cursor": "pointer"
    };
    var i=0;
//    var circle=R.circle(100,100,50); 
//    circle.attr("fill": "#f00");  
    for (var state in china) {
//		china[state]['path'].color = Raphael.getColor(0.9);
//		china[state]['path'].color = Raphael.getColor(0.9);
				
        (function (st, state) {
			
			//获取当前图形的中心坐标
            var xx = st.getBBox().x + (st.getBBox().width / 2);
            var yy = st.getBBox().y + (st.getBBox().height / 2);
			
			
			
            
			//写入文字
			china[state]['text'] = R.text(xx, yy, china[state]['name']).attr(textAttr);
			var fillcolor = colors[arr[i]];
		//	st.attr({fill:fillcolor});//填充背景色
			st[0].onmouseover = function () {
                st.animate({fill: fillcolor, stroke: "#eee"}, 500);
				china[state]['text'].toFront();
                R.safari();
             
            };
            st[0].onmouseout = function () {
                st.animate({fill: fillcolor, stroke: "#eee"}, 500);
				china[state]['text'].toFront();
                R.safari();
                
            };
          
           
            
            
				
         })(china[state]['path'], state);
         i++;
    }
    

}




</script>



</body>
</html>