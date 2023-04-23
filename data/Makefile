VAR_ALL_TARGETS = `grep -E "figure.+\.pdf:" Makefile | cut -d ' ' -f1 | tr -d ':' | sed '/^VAR/d; /SHELL/d;' | tr '\n' ' '`
VAR_LATENCY_FLAGS = --plot-type line --data-type latency --x-axis-scalar 0.001 --xlabel "Elapsed Time (s)" --ylabel "average latency (μs)"
VAR_HISTOGRAM_FLAGS = --data-type histogram --plot-type scatter --xlabel "RTT Latency (μs)" --ylabel "fraction of observations"
VAR_CDF_FLAGS = --data-type cdf --plot-type line --xlabel "RTT Latency (μs)" --ylabel "fraction of observations"
VAR_BOXPLOT_FLAGS = --plot-type boxplot --data-type boxplot --ylabel "latency (μs)"
VAR_DEFAULT_SIZE = --fig-width 4 --fig-height 4
VAR_WIDE_SIZE = --fig-width 9 --fig-height 4
VAR_WIDE_LATENCY_ymax = --ymax 2000

SHELL = /bin/bash -x

.PHONY: all_figures
all_figures:
	for target in $(VAR_ALL_TARGETS) ; do \
		$(MAKE) $$target ; \
	done; \
	$(MAKE) ready_figures

.PHONY: ready_figures
ready_figures:
	cd figures/all && find . -name "*.pdf" -exec rm {} \; && find .. -name "*.pdf" -exec cp {} . \; && find . -name "*.pdf" -exec basename {} \; | xargs -n 1 printf '\includegraphics[width=\\columnwidth]{figures/processed_latency_figures/%s}\n' | sort > all_figures.tex;

VAR_FIG1_Y_LIM = --ymin 200 --ymax 1500
VAR_FIG2_Y_LIM = --ymin 200 --ymax 800

figures/aws/aws_fig1.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_fig1.pdf --title "AWS Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_WIDE_LATENCY_ymax) $(VAR_FIG1_Y_LIM) ./processed_data/aws/latency_same_subnet.csv

figures/azure/azure_fig1.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/azure/azure_fig1.pdf --title "Azure Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_WIDE_LATENCY_ymax) $(VAR_FIG1_Y_LIM) ./processed_data/azure/latency_same_subnet.csv

figures/gcp/gcp_fig1.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/gcp/gcp_fig1.pdf --title "GCP Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_WIDE_LATENCY_ymax) $(VAR_FIG1_Y_LIM) ./processed_data/gcp/latency_same_subnet.csv

figures/aws/aws_fig2.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/aws/aws_fig2.pdf --title "AWS Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_WIDE_LATENCY_ymax) $(VAR_FIG2_Y_LIM) --start-bucket 5000 --end-bucket 5600 ./processed_data/aws/latency_same_subnet.csv

figures/azure/azure_fig2.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/azure/azure_fig2.pdf --title "Azure Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_WIDE_LATENCY_ymax) $(VAR_FIG2_Y_LIM) --start-bucket 4000 --end-bucket 4600 ./processed_data/azure/latency_same_subnet.csv

figures/gcp/gcp_fig2.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/gcp/gcp_fig2.pdf --title "GCP Same Subnet" $(VAR_LATENCY_FLAGS) $(VAR_WIDE_SIZE) $(VAR_WIDE_LATENCY_ymax) $(VAR_FIG2_Y_LIM) --start-bucket 6000 --end-bucket 6600 ./processed_data/gcp/latency_same_subnet.csv

figures/agg/agg_figure_3a.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3a.pdf --title "Same Subnet Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_HISTOGRAM_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_same_subnet.csv

figures/agg/agg_figure_3b.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3b.pdf --title "Cross Subnet Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_HISTOGRAM_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_cross_subnet.csv

figures/agg/agg_figure_3c.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3c.pdf --title "Cross AZ Latency Histogram" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 400 $(VAR_HISTOGRAM_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_cross_az.csv

figures/agg/agg_figure_3_5a.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5a.pdf --title "Same Subnet Latency CDF" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_CDF_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_same_subnet.csv
 
figures/agg/agg_figure_3_5b.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5b.pdf --title "Cross Subnet Latency CDF" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 225 $(VAR_CDF_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_cross_subnet.csv

figures/agg/agg_figure_3_5c.pdf: ./process_hist.py Makefile
	./process_hist.py -o ./figures/agg/agg_figure_3_5c.pdf --title "Cross AZ Latency CDF" $(VAR_DEFAULT_SIZE) --dir-labels 1 --end-bucket 400 $(VAR_CDF_FLAGS) ./processed_data/{aws,gcp,azure}/histogram_cross_az.csv

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

.PHONY: data_processing aws_data azure_data gcp_data aws_data_m5_large different_payload_processing
data_processing: ../bin/processing aws_data azure_data gcp_data

VAR_PROCESSING_DEFAULT_ARGS = -experiments_json ./experiments.json

aws_data: ./experiments.json
	../bin/processing -csvdir data/raw_aws_6hr_100pings_2vcpu8GB_1024bytes $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/aws

aws_data_m5_large: ./experiments.json
	../bin/processing -csvdir data/raw_aws_10min_100pings_m5large_1024bytes $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/aws_m5large

azure_data: ./experiments.json
	../bin/processing -csvdir data/raw_azure_6hrs_100pings_2vcpu8GB_1024bytes $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/azure

gcp_data: ./experiments.json
	../bin/processing -csvdir data/raw_gcp_6hr_100pings_2vcpu8GB_1024bytes $(VAR_PROCESSING_DEFAULT_ARGS) -outdir processed_data/gcp

different_payload_processing: ./experiments.json
	for cloud in aws azure gcp ; do  \
		for size in 512 1024 2048 4096 ; do \
			mkdir -p processed_data/different_payload/$$cloud/$$size; \
			../bin/processing -csvdir `echo data/different_payload_size/clt_raw_$${cloud}_1hr_$${size}bytes` $(VAR_PROCESSING_DEFAULT_ARGS) -outdir `echo processed_data/different_payload/$${cloud}/$${size}`; \
		done; \
	done;

clean:
	find figures -name "*.pdf" -exec rm {} \;