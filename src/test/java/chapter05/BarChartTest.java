package chapter05;

import java.awt.Color;
import java.awt.Font;
import java.io.PrintWriter;

import javax.servlet.http.HttpSession;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickMarkPosition;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.servlet.ServletUtilities;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.Year;
import org.jfree.data.xy.IntervalXYDataset;

public class BarChartTest {
	@SuppressWarnings("deprecation")
	private static IntervalXYDataset createDataset(){
		TimeSeriesCollection datase=new TimeSeriesCollection();
		TimeSeries series=new TimeSeries("������",Year.class);
		series.add(new Year(2000),5000);
		series.add(new Year(2001),5500);
		series.add(new Year(2002),6000);
		series.add(new Year(2003),8000);
		series.add(new Year(2004),7000);
		series.add(new Year(2005),7500);
		series.add(new Year(2006),6000);
		series.add(new Year(2007),12000);
		series.add(new Year(2008),8000);
		datase.addSeries(series);
		return datase;
	}
	
	private static JFreeChart createChart(IntervalXYDataset xyDataSet){
		JFreeChart jFreeChart=ChartFactory.createXYBarChart(
				"���꾭��ծ�ķ�ծ������",				//����
				"��", 							//x���ǩ
				true,							//�Ƿ���ʾ����
				"���/��Ԫ",						//y���ǩ
				xyDataSet,						//��ݼ�
				PlotOrientation.VERTICAL,		//��ʾ����
				true,							//�Ƿ���ʾͼ��
				false,							//�Ƿ��й�����
				false);							//�Ƿ�������
		jFreeChart.setBackgroundPaint(Color.white);
		jFreeChart.addSubtitle(new TextTitle("������ҵ����",new Font("Dialog",Font.ITALIC,10)));
		XYPlot plot=(XYPlot) jFreeChart.getPlot();
		XYBarRenderer renderer=(XYBarRenderer) plot.getRenderer();
		renderer.setMargin(0.50);
		DateAxis axis=(DateAxis) plot.getDomainAxis();
		axis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
		axis.setLowerMargin(0.01);
		axis.setUpperMargin(0.01);
		return jFreeChart;
		
		
	}
	
	public static String generateBarChart(HttpSession session,PrintWriter pw){
		String filename=null;
		IntervalXYDataset xyDataset=createDataset();
		JFreeChart chart=createChart(xyDataset);
		ChartRenderingInfo info=new ChartRenderingInfo();
		try {
			filename=ServletUtilities.saveChartAsPNG(chart, 500,300, info, session);
			ChartUtilities.writeImageMap(pw, filename, info, false);
			pw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return filename;
	}
	
	
	
}
