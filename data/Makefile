VAR_ALL_TARGETS = $(shell grep -E "figure.+\.pdf:" Makefile | cut -d ' ' -f1 | tr -d ':' | sed '/^VAR/d; /SHELL/d;' | tr '\n' ' ')
VAR_LATENCY_FLAGS = --plot-type line --data-type latency --x-axis-scalar 0.001 --xlabel "Elapsed Time (s)" --ylabel "average latency (μs)"
VAR_HISTOGRAM_FLAGS = --data-type histogram --plot-type scatter --xlabel "Latency (μs)" --ylabel "fraction of observations" --enable-cloud-markers
VAR_CDF_FLAGS = --data-type cdf --plot-type line --xlabel "Latency (μs)" --ylabel "fraction of observations"
VAR_BOXPLOT_FLAGS = --plot-type boxplot --data-type boxplot --ylabel "latency (μs)"

VAR_HALF_HEIGHT_SIZE = --fig-width 4 --fig-height 2.2
VAR_DEFAULT_SIZE = --fig-width 4 --fig-height 3.6
VAR_WIDE_SIZE = --fig-width 9 --fig-height 2
# VAR_WIDE_SIZE = --fig-width 9000 --fig-height 200

SHELL = /bin/bash

.PHONY: all_figures
all_figures: $(VAR_ALL_TARGETS)
	$(MAKE) ready_figures

.PHONY: ready_figures
ready_figures:
	cd figures/all && find . -name "*.pdf" -exec rm {} \; && find .. -name "*.pdf" -exec cp {} . \; && find . -name "*.pdf" -exec basename {} \; | xargs -n 1 printf '\includegraphics[width=\\columnwidth]{figures/processed_latency_figures/%s}\n' | sort > all_figures.tex;

VAR_FIG1_Y_LIM = --ymin 250 --ymax 1400
VAR_FIG2_Y_LIM = --ymin 200 --ymax 1000

figures/aws/aws_fig1.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_fig1.pdf --title "AWS Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_FIG1_Y_LIM) ./processed_data/aws/latency_same_subnet.csv

figures/aws/aws_fig1_zoom.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_fig1_zoom.pdf --title "AWS Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) --ymin 250 --ymax 400 ./processed_data/aws/latency_same_subnet.csv

figures/azure/azure_fig1.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/azure/azure_fig1.pdf --title "Azure Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_FIG1_Y_LIM) ./processed_data/azure/latency_same_subnet.csv

figures/azure/azure_fig1_zoom.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/azure/azure_fig1_zoom.pdf --title "Azure Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) --ymin 500 --ymax 1400 ./processed_data/azure/latency_same_subnet.csv

figures/gcp/gcp_fig1.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/gcp/gcp_fig1.pdf --title "GCP Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_FIG1_Y_LIM) ./processed_data/gcp/latency_same_subnet.csv

figures/gcp/gcp_fig1_zoom.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/gcp/gcp_fig1_zoom.pdf --title "GCP Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) --ymin 500 --ymax 600 ./processed_data/gcp/latency_same_subnet.csv


VAR_FIG2_AWS_BUCKETS=--start-bucket 5000 --end-bucket 5600
VAR_FIG2_AZURE_BUCKETS=--start-bucket 3800 --end-bucket 4400
VAR_FIG2_GCP_BUCKETS=--start-bucket 16800 --end-bucket 17400

figures/aws/aws_fig2.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_fig2.pdf --title "AWS Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_DEFAULT_SIZE) $(VAR_FIG2_Y_LIM) $(VAR_FIG2_AWS_BUCKETS) ./processed_data/aws/latency_same_subnet.csv

figures/azure/azure_fig2.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/azure/azure_fig2.pdf --title "Azure Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_DEFAULT_SIZE) $(VAR_FIG2_Y_LIM) $(VAR_FIG2_AZURE_BUCKETS) ./processed_data/azure/latency_same_subnet.csv

figures/gcp/gcp_fig2.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/gcp/gcp_fig2.pdf --title "GCP Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_DEFAULT_SIZE) $(VAR_FIG2_Y_LIM) $(VAR_FIG2_GCP_BUCKETS) ./processed_data/gcp/latency_same_subnet.csv

figures/aws/aws_fig2_zoom.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_fig2_zoom.pdf --title "AWS Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_DEFAULT_SIZE) --ymin 250 --ymax 400 $(VAR_FIG2_AWS_BUCKETS) ./processed_data/aws/latency_same_subnet.csv

figures/azure/azure_fig2_zoom.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/azure/azure_fig2_zoom.pdf --title "Azure Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_DEFAULT_SIZE) --ymin 500 --ymax 900 $(VAR_FIG2_AZURE_BUCKETS) ./processed_data/azure/latency_same_subnet.csv

figures/gcp/gcp_fig2_zoom.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/gcp/gcp_fig2_zoom.pdf --title "GCP Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_DEFAULT_SIZE) --ymin 400 --ymax 525 $(VAR_FIG2_GCP_BUCKETS) ./processed_data/gcp/latency_same_subnet.csv

figures/agg/agg_figure_3a.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3a.pdf --title "Same Subnet Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_HISTOGRAM_FLAGS) --enable-cloud-markers ./processed_data/{aws,gcp,azure}/histogram_same_subnet.csv

figures/agg/agg_figure_3b.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3b.pdf --title "Cross Subnet Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_HISTOGRAM_FLAGS) --enable-cloud-markers ./processed_data/{aws,gcp,azure}/histogram_cross_subnet.csv

figures/agg/agg_figure_3c.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3c.pdf --title "Cross AZ Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 400 $(VAR_HISTOGRAM_FLAGS) --enable-cloud-markers ./processed_data/{aws,gcp,azure}/histogram_cross_az.csv

figures/agg/agg_figure_3_5a.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5a.pdf --title "Same Subnet Latency CDF" $(VAR_HALF_HEIGHT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_CDF_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_same_subnet.csv
 
figures/agg/agg_figure_3_5b.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5b.pdf --title "Cross Subnet Latency CDF" $(VAR_HALF_HEIGHT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_CDF_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_cross_subnet.csv

figures/agg/agg_figure_3_5c.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5c.pdf --title "Cross AZ Latency CDF" $(VAR_HALF_HEIGHT_SIZE) --dir-labels 1 --end-bucket 400 $(VAR_CDF_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_cross_az.csv

VAR_FIG3_5_tail_LIMS= --ymin 0.95 --ymax 1.0

figures/agg/agg_figure_3_5a_tail.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5a_tail.pdf --title "Same Subnet Latency CDF" $(VAR_HALF_HEIGHT_SIZE) --dir-labels 1 --end-bucket 500 $(VAR_CDF_FLAGS) $(VAR_FIG3_5_tail_LIMS) ./processed_data/{aws,gcp,azure}/histogram_same_subnet.csv
 
figures/agg/agg_figure_3_5b_tail.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5b_tail.pdf --title "Cross Subnet Latency CDF" $(VAR_HALF_HEIGHT_SIZE) --dir-labels 1 --end-bucket 500 $(VAR_CDF_FLAGS) $(VAR_FIG3_5_tail_LIMS) ./processed_data/{aws,gcp,azure}/histogram_cross_subnet.csv

figures/agg/agg_figure_3_5c_tail.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5c_tail.pdf --title "Cross AZ Latency CDF" $(VAR_HALF_HEIGHT_SIZE) --dir-labels 1 --end-bucket 1000 $(VAR_CDF_FLAGS) $(VAR_FIG3_5_tail_LIMS) ./processed_data/{aws,gcp,azure}/histogram_cross_az.csv

figures/agg/agg_figure_4_talking_to_self.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_4_talking_to_self.pdf --title "Talking to self Latency" $(VAR_DEFAULT_SIZE) --dir-labels 1 $(VAR_BOXPLOT_FLAGS) --xlabel cloud ./processed_data/{aws,gcp,azure}/latency_talking_to_self.csv

figures/agg/agg_figure_4_same_subnet.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_4_same_subnet.pdf --title "Same Subnet Latency" $(VAR_DEFAULT_SIZE) --dir-labels 1 $(VAR_BOXPLOT_FLAGS) --xlabel cloud ./processed_data/{aws,gcp,azure}/latency_same_subnet.csv

figures/agg/agg_figure_4_cross_subnet.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_4_cross_subnet.pdf --title "Cross Subnet Latency" $(VAR_DEFAULT_SIZE) --dir-labels 1  $(VAR_BOXPLOT_FLAGS) --xlabel cloud ./processed_data/{aws,gcp,azure}/latency_cross_subnet.csv

figures/agg/agg_figure_4_cross_az.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_4_cross_az.pdf --title "Cross AZ Latency" $(VAR_DEFAULT_SIZE) --dir-labels 1 $(VAR_BOXPLOT_FLAGS) --xlabel cloud ./processed_data/{aws,gcp,azure}/latency_cross_az.csv

figures/aws/aws_figure_5_same_subnet.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_figure_5_same_subnet.pdf --title "Same Subnet Latency" $(VAR_DEFAULT_SIZE) --dir-labels 1 $(VAR_BOXPLOT_FLAGS) --xlabel "VM Size" ./processed_data/aw*/latency_same_subnet.csv

.PHONY: figure_6
figure_6.pdf: ./process_hist.py Makefile
	for size in 512 1024 2048 4096 ; do \
		./process_hist.py -o `echo ./figures/agg/figure_6_same_subnet_$${size}_bytes.pdf` --title "Same Subnet Latency $$size bytes" $(VAR_DEFAULT_SIZE) --dir-labels 2 $(VAR_BOXPLOT_FLAGS) --ymin 200 --ymax 1000 --xlabel cloud ./processed_data/different_payload/*/$$size/latency_same_subnet.csv; \
	done; \
	\
	for cloud in aws azure gcp ; do  \
		./process_hist.py -o `echo ./figures/$$cloud/figure_6_payload_sizes_$${cloud}.pdf` --title "Same Subnet Latency $$cloud" $(VAR_DEFAULT_SIZE) --dir-labels 1 $(VAR_BOXPLOT_FLAGS) --ymin 200 --ymax 1000 --xlabel "size (bytes)" ./processed_data/different_payload/$$cloud/*/latency_same_subnet.csv; \
	done;

VAR_FIG_7_EAST_EAST_LATENCY = --ymin 5000 --ymax 13000
VAR_FIG_7_EAST_WEST_LATENCY = --ymin 30000 --ymax 65000

.PHONY: figure_7
figure_7.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/figure_7_aws_cross_region_east_east.pdf --title 'Cross Region East-East Latency aws' $(VAR_WIDE_SIZE) $(VAR_LATENCY_FLAGS) $(VAR_FIG_7_EAST_EAST_LATENCY) ./processed_data/aws/latency_cross_region_east-east.csv; \
	./process_hist.py -o ./figures/aws/figure_7_aws_cross_region_east_west.pdf --title 'Cross Region East-West Latency aws' $(VAR_WIDE_SIZE) $(VAR_LATENCY_FLAGS) $(VAR_FIG_7_EAST_WEST_LATENCY) ./processed_data/aws/latency_cross_region_east1-west.csv; \
	./process_hist.py -o ./figures/azure/figure_7_azure_cross_region_east_east.pdf --title 'Cross Region East-East Latency azure' $(VAR_WIDE_SIZE) $(VAR_LATENCY_FLAGS) $(VAR_FIG_7_EAST_EAST_LATENCY) ./processed_data/azure/latency_cross_region_east-east.csv; \
	./process_hist.py -o ./figures/azure/figure_7_azure_cross_region_east_west.pdf --title 'Cross Region East-West Latency azure' $(VAR_WIDE_SIZE) $(VAR_LATENCY_FLAGS) $(VAR_FIG_7_EAST_WEST_LATENCY) ./processed_data/azure/latency_cross_region_east1-west.csv; \
	./process_hist.py -o ./figures/gcp/figure_7_gcp_cross_region_east_east.pdf --title 'Cross Region East-East Latency gcp' $(VAR_WIDE_SIZE) $(VAR_LATENCY_FLAGS) $(VAR_FIG_7_EAST_EAST_LATENCY) ./processed_data/gcp/latency_cross_region_east-east.csv; \
	./process_hist.py -o ./figures/gcp/figure_7_gcp_cross_region_east_west.pdf --title 'Cross Region East-West Latency gcp' $(VAR_WIDE_SIZE) $(VAR_LATENCY_FLAGS) $(VAR_FIG_7_EAST_WEST_LATENCY) ./processed_data/gcp/latency_cross_region_east1-west.csv;

VAR_CLOUDS=aws azure gcp

.PHONY: three_lines_one_hour
figures_three_lines_one_hour.pdf: ./process_hist.py Makefile ./processed_data/three_runs_comparison/*/histogram_cross_*.csv 
	./process_hist.py -o figures/agg/three_run_cmp_same_subnet.pdf --title "Same Subnet Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 250 $(VAR_HISTOGRAM_FLAGS) ./processed_data/three_runs_comparison/*/histogram_same_subnet.csv; \
	./process_hist.py -o figures/agg/three_run_cmp_cross_subnet.pdf --title "Cross Subnet Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 500 $(VAR_HISTOGRAM_FLAGS) ./processed_data/three_runs_comparison/*/histogram_cross_subnet.csv; \
	./process_hist.py -o figures/agg/three_run_cmp_cross_az.pdf --title "Cross AZ Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --start-bucket 50 --end-bucket 450 $(VAR_HISTOGRAM_FLAGS) ./processed_data/three_runs_comparison/*/histogram_cross_az.csv; \

VAR_DP_TARGETS = `grep -E "^data_processing_" Makefile | cut -d ' ' -f1 | tr -d ':' | sed '/^VAR/d; /SHELL/d;' | tr '\n' ' '`

dp2: 
	$(MAKE) $(VAR_DP_TARGETS)

.PHONY: data_processing data_processing_aws_data data_processing_azure_data data_processing_gcp_data data_processing_aws_data_m5_large data_processing_different_payload_processing data_processing_separate_time_runs_setup
data_processing: ../bin/processing data_processing_aws_data data_processing_azure_data data_processing_gcp_data data_processing_separate_time_runs_setup data_processing_aws_data_m5_large data_processing_different_payload_processing

VAR_PROCESSING_DEFAULT_ARGS = -experiments_json ./experiments.json
VAR_SUFFIX_DISKIO=_100pings_2vcpu8GB_1024bytes
VAR_SUFFIX_NODISKIO=_nodiskio_2vcpu_8GBRam_1024bytes_100pings

data_processing_aws_data: ./experiments.json
	../bin/processing -csvdir data/raw_aws_6hr_nodiskio_2vcpu_8GBRam_1024bytes_100pings $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/aws

data_processing_aws_data_m5_large: ./experiments.json
	../bin/processing -csvdir data/raw_aws_10min_100pings_m5large_1024bytes $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/aws_m5large

data_processing_azure_data: ./experiments.json
	../bin/processing -csvdir data/raw_azure_6hr_nodiskio_2vcpu_8GBRam_1024bytes_100pings $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/azure

data_processing_gcp_data: ./experiments.json
	../bin/processing -csvdir data/raw_gcp_6hr_nodiskio_2vcpu_8GBRam_1024bytes_100pings $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/gcp

data_processing_different_payload_processing: ./experiments.json
	for cloud in aws azure gcp ; do  \
		for size in 512 1024 2048 4096 ; do \
			mkdir -p processed_data/different_payload/$$cloud/$$size; \
			../bin/processing -csvdir `echo data/different_payload_size/clt_raw_$${cloud}_1hr_$${size}bytes` $(VAR_PROCESSING_DEFAULT_ARGS) -outdir `echo processed_data/different_payload/$${cloud}/$${size}`; \
		done; \
	done;

data_processing_separate_time_runs_preprocess: ./experiments.json data/raw_*_6hr_nodiskio_2vcpu_8GBRam_1024bytes_100pings/* data/raw_*_6hr_100pings_2vcpu8GB_1024bytes/* data/different_payload_size/clt_raw_*_1hr_1024bytes
	rm -rf ./data/three_runs_comparison; \
	for cloud in aws azure gcp ; do  \
		mkdir -p data/three_runs_comparison/$$cloud; \
		for file in `find data/raw_$${cloud}_6hr_nodiskio_2vcpu_8GBRam_1024bytes_100pings data/raw_$${cloud}_6hr_100pings_2vcpu8GB_1024bytes data/different_payload_size/clt_raw_$${cloud}_1hr_1024bytes -name "*.csv.gz" | tr '\n' ' '`; do \
			gunzip -k -c $$file | awk -F ',' 'BEGIN { print "thisNodeId,roundNumber,remoteNodeId,startTime,endTime" } { if ($$2 <= 3600000) { print $$0 } }' | gzip -1 -c - > `printf "./data/three_runs_comparison/$${cloud}/$${cloud}_%s" $$(basename $${file})`; \
		done; \
	done;

data_processing_separate_time_runs_setup: ./experiments.json Makefile ./data/three_runs_comparison/aws/* ./data/three_runs_comparison/azure/* ./data/three_runs_comparison/gcp/*
	for cloud in aws azure gcp ; do  \
		mkdir -p processed_data/three_runs_comparison/$$cloud; \
		../bin/processing -csvdir ./data/three_runs_comparison/$$cloud $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/three_runs_comparison/$$cloud; \
	done;

clean:
	find figures -name "*.pdf" -exec rm {} \;