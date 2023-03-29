package main

import (
	"fmt"
	"github.com/UNH-DistSyS/UNH-CLT/data_processing"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"os"
	"sort"
)

func PlotHistogram(hist *data_processing.Histogram, filename, title string, normalizeYAxis bool, offset int) error {
	width := 2000
	barStyle := chart.Style{
		FillColor:   drawing.ColorFromHex("13c158"),
		StrokeColor: drawing.ColorFromHex("c19641"),
		StrokeWidth: 1,
	}

	// Prepare data for the histogram
	var values []chart.Value
	data := hist.GetHistogram()
	barWidth := (width / len(data)) - 4

	// sort the keys
	keys := make([]int, 0, len(data))
	for k := range data {
		if k*hist.GetHistogramBucketWith() >= offset {
			keys = append(keys, k)
		}
	}
	sort.Ints(keys)

	norm := hist.Count()
	max := 0.0
	// write the data
	for i, key := range keys {
		value := float64(data[key])

		if normalizeYAxis {
			value = value / float64(norm)
		}

		if max < value {
			max = value
		}

		chartVal := chart.Value{
			Value: value,
			Style: barStyle,
		}

		if i%2 == 0 {
			chartVal.Label = fmt.Sprintf("%2.1f", float64(key*hist.GetHistogramBucketWith())/1000.0)
		}

		values = append(values, chartVal)
	}

	// Create the histogram chart
	graph := chart.BarChart{
		Title:      title,
		TitleStyle: chart.StyleShow(),
		Background: chart.Style{
			Padding: chart.Box{
				Top:   100,
				Right: 20,
			},
		},
		Width:    width,
		Height:   700,
		BarWidth: barWidth,
		Bars:     values,
	}

	if normalizeYAxis {
		graph.YAxis.Range = &chart.ContinuousRange{
			Min: 0,
			Max: max,
		}
	} else {
		t1 := int(max) / 10
		t1rem := t1 % 1000
		if t1rem >= 500 {
			t1 = t1 - t1rem + 1000
		} else {
			t1 = t1 - t1rem
		}

		ticks := make([]chart.Tick, 0)
		for i := 0; i < int(max); i += t1 {
			ticks = append(ticks, chart.Tick{Value: float64(i), Label: fmt.Sprintf("%d", i)})
		}

		graph.YAxis.Ticks = ticks
	}

	graph.YAxis.Style.Show = true
	graph.YAxis.GridMajorStyle.Show = true
	graph.YAxis.GridMinorStyle.Show = false

	graph.XAxis.Show = true
	graph.XAxis.FontSize = 8
	graph.XAxis.TextRotationDegrees = 45

	// Save the chart as a PNG file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	err = graph.Render(chart.PNG, file)
	if err != nil {
		return err
	}

	return nil
}

func PlotAggregatedLatencyOverTime(w *data_processing.WindowAggregator, filename, title string) error {
	width := 2000
	barStyle := chart.Style{
		FillColor:   drawing.ColorFromHex("13c158"),
		StrokeColor: drawing.ColorFromHex("13c158"),
		StrokeWidth: 1,
	}

	// Prepare data for the histogram
	var values []chart.Value
	data := w.GetAverageAggregates()
	barWidth := (width / len(data))

	// sort the keys
	keys := make([]int, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	max := 0.0

	// write the data
	for i, key := range keys {
		value := float64(data[key])

		if value > max {
			max = value
		}

		chartVal := chart.Value{
			Value: value,
			Style: barStyle,
		}

		if i%10 == 0 {
			chartVal.Label = fmt.Sprintf("%2.1f", float64(i*w.GetWindowWidth())/1000.0)
		}

		values = append(values, chartVal)
	}

	// Create the histogram chart
	graph := chart.BarChart{
		Title:      title,
		TitleStyle: chart.StyleShow(),
		Background: chart.Style{
			Padding: chart.Box{
				Top:   100,
				Right: 20,
			},
		},
		Width:    width,
		Height:   700,
		BarWidth: barWidth,
		Bars:     values,
	}

	graph.YAxis.Range = &chart.ContinuousRange{
		Min: 0,
		Max: max,
	}

	graph.YAxis.Style.Show = true
	graph.YAxis.GridMajorStyle.Show = true
	graph.YAxis.GridMinorStyle.Show = false

	graph.XAxis.Show = true
	graph.XAxis.FontSize = 8
	graph.XAxis.TextRotationDegrees = 45

	// Save the chart as a PNG file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	err = graph.Render(chart.PNG, file)
	if err != nil {
		return err
	}

	return nil
}
