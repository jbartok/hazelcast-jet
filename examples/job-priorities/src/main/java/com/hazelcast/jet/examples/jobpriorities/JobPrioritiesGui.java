package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
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
    private UUID entryListenerId;
    private JFrame frame;

    public JobPrioritiesGui(IMap<String, Long> hzMap, int hues) {
        this.hzMap = hzMap;
        this.colors = getColors(hues);
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
                int separatorIndex = event.getKey().indexOf(':');
                dataset.addValue(event.getValue(),
                        "",
                        event.getKey().substring(separatorIndex + 1) + ", prio. " +
                                event.getKey().substring(0, separatorIndex));
                topY[0] = max(topY[0], INITIAL_TOP_Y * (1 + event.getValue() / INITIAL_TOP_Y));
                yAxis.setRange(0, topY[0]);
            });
        };

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
        plot.getRenderer().setSeriesPaint(0, new Color(52, 119, 235));

        return plot;
    }

    public void stop() {
        frame.setVisible(false);
        frame.dispose();
    }
}
