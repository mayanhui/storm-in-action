package chapter05.web.chart;

import java.awt.Color;
import java.awt.Font;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpSession;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.servlet.ServletUtilities;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import chapter05.web.model.DataSet;
import chapter05.web.util.JDBCUtils;

public class BarChart {
	private static Map<Integer, CategoryDataset> createDataset() {

		Map<Integer, List<DataSet>> dataMap = JDBCUtils.getData(new Date());
		Map<Integer, CategoryDataset> cdsMap = new TreeMap<Integer, CategoryDataset>();
		DefaultCategoryDataset cgd = null;
		for (Integer i : dataMap.keySet()) {
			cgd = new DefaultCategoryDataset();
			for (DataSet ds : dataMap.get(i)) {
				cgd.addValue(ds.getCount(), "异常数目", ds.getTimes() + "");
			}
			cdsMap.put(i, cgd);
		}
		return cdsMap;
	}

	public static void main(String[] args) {
		createDataset();
	}

	private static JFreeChart createChart(CategoryDataset xyDataSet) {

		JFreeChart jFreeChart = ChartFactory.createBarChart("异常账户统计报告", // 标题
				"统计次数", // x轴标签 //是否显示日期
				"异常帐号个数/个", // y轴标签
				xyDataSet, // 数据集
				PlotOrientation.VERTICAL, // 显示方向
				true, // 是否显示图例
				false, // 是否有工具条
				false); // 是否有链接
		jFreeChart.setBackgroundPaint(Color.white);
		jFreeChart.addSubtitle(new TextTitle("异常帐号报表", new Font("Dialog",
				Font.ITALIC, 10)));
		CategoryPlot cplot = (CategoryPlot) jFreeChart.getPlot();
		BarRenderer render = (BarRenderer) cplot.getRenderer();
		render.setItemMargin(0.10);
		render.setMaximumBarWidth(0.05);
		// render.set
		CategoryAxis axis = cplot.getDomainAxis();
		axis.setLowerMargin(0.1);
		axis.setUpperMargin(0.1);
		return jFreeChart;

	}

	public static void generateBarChart(HttpSession session, PrintWriter pw) {

		List<String> fileNames = new ArrayList<String>();
		try {
			String filename = null;
			Map<Integer, CategoryDataset> datasets = createDataset();
			for (Integer i : datasets.keySet()) {
				JFreeChart chart = createChart(datasets.get(i));
				ChartRenderingInfo info = new ChartRenderingInfo();
				filename = ServletUtilities.saveChartAsPNG(chart, 500, 300,
						info, session);
				fileNames.add(filename);
				System.out.println(filename);
				ChartUtilities.writeImageMap(pw, filename, info, false);
				pw.flush();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		session.setAttribute("filenames", fileNames);

	}

}
