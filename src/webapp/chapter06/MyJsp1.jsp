<%@ page language="java" import="java.util.*,com.cyt.model.*" pageEncoding="UTF-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta charset="utf-8">
<title>演示：使用raphael.js绘制交通地图</title>
<link rel="stylesheet" type="text/css" href="images/main.css" />
<style type="text/css">
.demo{width:760px; height:500px; margin:30px auto 0 auto; font-size:14px;}
.demo p{line-height:30px}
</style>
<script type="text/javascript" src="js/raphael.js"></script>
<script type="text/javascript" src="js/chinamapPath.js"></script>
<script type="text/javascript" src="jquery.js"></script>
</head>

<body>

<div id="main">
   <h2 class="top_title"><a href="#">使用raphael.js绘制交通地图</a></h2>
   <div class="demo">
   		<div id="map"></div>
   		
   </div>
	
</div>

<script type="text/javascript">

$(function(){ 
    $.get("json.php",function(json){//获取数据 
    var data = string2Array(json);//转换数组 
     
    var flag; 
    var arr = new Array();//定义新数组，对应等级 
    for(var i=0;i<data.length;i++){ 
        var d = data[i]; 
        if(d<100){ 
            flag = 0; 
        }else if(d>=100 && d<500){ 
            flag = 1; 
        }else if(d>=500 && d<2000){ 
            flag = 2; 
        }else if(d>=2000 && d<5000){ 
            flag = 3; 
        }else if(d>=5000 && d<10000){ 
            flag = 4; 
        }else{ 
            flag = 5; 
        } 
        arr.push(flag); 
    } 
    //定义颜色 
    var colors = ["#d7eef8","#97d6f5","#3fbeef","#00a2e9","#0084be","#005c86"]; 
     
    //调用绘制地图方法 
    var R = Raphael("map", 600, 500); 
    paintMap(R); 
     
    var textAttr = { 
        "fill": "#000", 
        "font-size": "12px", 
        "cursor": "pointer" 
    }; 
             
    var i=0; 
    for (var state in china) { 
        china[state]['path'].color = Raphael.getColor(0.9); 
        (function (st, state) { 
             
            //获取当前图形的中心坐标 
            var xx = st.getBBox().x + (st.getBBox().width / 2); 
            var yy = st.getBBox().y + (st.getBBox().height / 2); 
             
           
            //写入文字 
            china[state]['text'] = R.text(xx, yy, china[state]['name']).attr(textAttr); 
             
            var fillcolor = colors[arr[i]];//获取对应的颜色 
             
            st.attr({fill:fillcolor});//填充背景色 
             
            st[0].onmouseover = function () { 
                st.animate({fill: "#fdd", stroke: "#eee"}, 500); 
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
    }); 
});

function string2Array(string) {  
    eval("var result = " + decodeURI(string));  
    return result;  
}

</script>

 //<% 
 //   List l = (List)request.getAttribute("st");
   // 		for(Object o: l){
 	//	Car c=(Car)o;
  	//	out.println("<tr>");
  	//		out.println("<td>" +"区域："+c.getDirectId() + "</td>");
   	//	out.println("<td>" + "车辆个数："+c.getCount() + "</td>");
   	//		out.print("<td>");
	//			out.print("</td>");
   	//	out.println("</tr>");  		
   	//	}
//  %>

</body>
</html>