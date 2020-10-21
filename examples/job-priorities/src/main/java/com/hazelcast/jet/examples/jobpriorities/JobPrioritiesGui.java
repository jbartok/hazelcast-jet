package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.UUID;

import static java.lang.Long.max;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

public class JobPrioritiesGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1200;
    private static final int WINDOW_HEIGHT = 650;
    private static final int INITIAL_TOP_Y = 30_000_000;

    private final IMap<String, Long> hzMap;
    private final Color[] colors;
    private final int[] priorities;
    private UUID entryListenerId;
    private JFrame frame;

    public JobPrioritiesGui(IMap<String, Long> hzMap, int... priorities) {
        this.hzMap = hzMap;
        this.colors = getColors(priorities.length);
        this.priorities = priorities;
        EventQueue.invokeLater(this::startGui);
    }

    private Color[] getColors(int hues) {
        Color[] colors = new Color[hues];
        for (int i = 0; i < hues; i++) {
            colors[i] = new Color(128 - i * 10, 0, 128 + i * 10);
        }
        return colors;
    }

    private void startGui() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        JFreeChart chart = createChart(dataset);
        frame = createJFrame(chart);

        CategoryPlot plot = createPlot(chart);
        ValueAxis yAxis = plot.getRangeAxis();
        long[] topY = {INITIAL_TOP_Y};
        EntryUpdatedListener<String, Long> entryUpdatedListener = event -> {
            EventQueue.invokeLater(() -> {
                dataset.addValue(event.getValue(),
                        "",
                        event.getKey());
                topY[0] = max(topY[0], INITIAL_TOP_Y * (1 + event.getValue() / INITIAL_TOP_Y));
                yAxis.setRange(0, topY[0]);
            });
        };
        for (int i = 0; i < priorities.length; i++) {
            String jobName = JobPriorities.jobName(i, priorities[i]);
            dataset.addValue(0, "", jobName);
        }
        entryListenerId = hzMap.addEntryListener(entryUpdatedListener, true);
    }

    private JFreeChart createChart(CategoryDataset dataset) {
        return ChartFactory.createBarChart(
                null, null, "Results", dataset,
                PlotOrientation.HORIZONTAL, false, true, true);
    }

    private JFrame createJFrame(JFreeChart chart) {
        JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Job Priorities");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        frame.add(new ChartPanel(chart));
        frame.setVisible(true);
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent) {
                hzMap.removeEntryListener(entryListenerId);
            }
        });
        return frame;
    }

    private CategoryPlot createPlot(JFreeChart chart) {
        CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.DARK_GRAY);
        plot.setRangeGridlinePaint(Color.DARK_GRAY);
        CustomBarRenderer renderer = new CustomBarRenderer(colors);
        renderer.setSeriesPaint(0, new Color(52, 119, 235));
        plot.setRenderer(renderer);
        return plot;
    }

    public void stop() {
        frame.setVisible(false);
        frame.dispose();
    }

    static class CustomBarRenderer extends BarRenderer {

        Color[] colors;

        public CustomBarRenderer(Color[] colors) {
            this.colors = colors;
        }

        @Override
        public Paint getItemPaint(int row, int column) {
            if (column < colors.length) {
                return colors[column];
            }
            return super.getItemPaint(row, column);
        }
    }

}
