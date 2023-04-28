#!/usr/bin/python3

from typing import Optional
from scipy.stats import sem
import matplotlib.pyplot as plt
import matplotlib.markers as plt_markers
import argparse
import numpy as np
import os

cloud_markers = {
    "aws": "o",
    "azure": "s",
    "gcp": "p"
}

cloud_colors = {
    "aws": "orange",
    "azure": "blue",
    "gcp": "green"
}


def load_csv_histogram(filename):
    data = np.loadtxt(filename, delimiter=",", skiprows=1)
    bucket = []
    lbl = []
    count = []
    sum = 0

    for dt in data:
        bucket.append(dt[0])
        lbl.append(dt[0] * 5)  # hardcoded as 5 microseconds per bucket
        count.append(dt[1])
        sum += dt[1]

    histval = count / sum

    return bucket, histval, lbl


def load_csv_latency(filename):
    return np.transpose(np.loadtxt(filename, delimiter=",", skiprows=1))


def dir_label_n(filename, n):
    if n == 0:
        return os.path.basename(filename)
    else:
        return dir_label_n(os.path.dirname(filename), n - 1)


parser = argparse.ArgumentParser()
parser.add_argument("--out", "-o", help="The output path", required=True)
parser.add_argument(
    "--start-bucket", help="The bucket index to start with", default=0, type=int
)
parser.add_argument(
    "--end-bucket",
    help="The bucket index to end with (exclusive)",
    default=None,
    type=int,
)
parser.add_argument("--title", help="The title of the graph")
parser.add_argument(
    "--dir-labels",
    help="Use the parent directory names of the files as series labels",
    type=int,
)
parser.add_argument(
    "--plot-type",
    help="What type of plot to generate (default scatter)",
    default="scatter",
)
parser.add_argument("--data-type", help="latency or histogram", default="histogram")
parser.add_argument(
    "--xlabel", help="The x axis label", type=str, required=True
)
parser.add_argument("--bucket-length-us", type=int, default=5, help="The length of a histogram bucket in μs")
parser.add_argument(
    "--ylabel", help="The y axis label", type=str, required=True
)
parser.add_argument(
    "--ymin", help="The minimum value of the y axis", type=float, default=None
)
parser.add_argument(
    "--ymax", help="The maximum value of the y axis", type=float, default=None
)
parser.add_argument(
    "--fig-width", help="The width of the figure in inches", type=float, required=True
)
parser.add_argument(
    "--fig-height", help="The height of the figure in inches", type=float, required=True
)
parser.add_argument(
    "--x-axis-scalar", help="An amount the multiply the x axis by", type=float, default=1
)
parser.add_argument(
    "--enable-cloud-markers", help="Use different markers for each cloud, determined by series name.", default=False, action='store_true'
)
parser.add_argument(
    "--legend-title", help="The title of the legend", default="Cloud", type=str,
)
parser.add_argument(
    "files", nargs="*", help="The histogram files to include in the plot."
)



args = parser.parse_args()

files: list[str] = args.files
output_path: str = args.out
start_bucket: int = args.start_bucket
end_bucket: int = args.end_bucket
plot_type: str = args.plot_type
bucket_length_us: int = args.bucket_length_us
ymin: Optional[int] = args.ymin
ymax: Optional[int] = args.ymax
x_scalar: float = args.x_axis_scalar

assert len(files) > 0

print(f"Inputs: {files}")

out_dirname = os.path.dirname(output_path)
if not os.path.exists(out_dirname):
    # original_umask = os.umask(0)
    os.makedirs(out_dirname, mode=755, exist_ok=False)
    # os.umask(original_umask)

print(f"Output path: {output_path}")

if files != None:
    fig = plt.figure(layout="constrained")
    if args.title is not None:
        pass
        # plt.title(args.title)
    histCount = 0
    for filename in files:
        assert os.path.exists(filename)
        assert os.path.isfile(filename)
        assert filename.endswith(".csv")
        histCount += 1

    currentHist = 0
    boxplot_series = list()
    plt.xlabel(args.xlabel)
    plt.ylabel(args.ylabel)


    for filename in files:
        match args.data_type:
            case "histogram":
                bucket, histval, lbl = load_csv_histogram(filename)

                extra_args = dict()

                x = np.array(bucket[start_bucket : end_bucket or len(bucket)])
                x = x * 3 * bucket_length_us
                y = histval[start_bucket : end_bucket or len(bucket)]
                l = lbl[start_bucket : end_bucket or len(bucket)]
                x = x + currentHist
                
            case "latency":
                latency = load_csv_latency(filename)
                x = latency[0][start_bucket:end_bucket]
                y = latency[1][start_bucket:end_bucket]

            case "cdf":
                bucket, histval, lbl = load_csv_histogram(filename)

                extra_args = dict()

                x = np.array(bucket[start_bucket : end_bucket or len(bucket)])
                x = x * 3 * bucket_length_us
                histval_percent = histval / np.sum(histval)
                histval_cumsum = np.cumsum(histval_percent)
                y = histval_cumsum[start_bucket : end_bucket or len(bucket)]
                l = lbl[start_bucket : end_bucket or len(bucket)]
                x = x + currentHist

            case "boxplot":
                latency = load_csv_latency(filename)
                x = latency[0]
                y = latency[1]

        x *= x_scalar

        match plot_type:
            case "scatter":
                extra_args = dict()
                if args.enable_cloud_markers and args.dir_labels is not None:
                    cloud_name = dir_label_n(filename, args.dir_labels)
                    extra_args["s"] = 10 # set marker size
                    extra_args["marker"] = cloud_markers.get(cloud_name) or "o"
                    extra_args["c"] = cloud_colors.get(cloud_name)
                series = plt.scatter(x, y, **extra_args)
                plt.ticklabel_format(axis='both', style='plain')
            case "line":
                extra_args = dict()
                if args.dir_labels is not None:
                    cloud_name = dir_label_n(filename, args.dir_labels)
                    extra_args["color"] = cloud_colors.get(cloud_name)
                    extra_args["label"] = cloud_name
                cur_min, cur_max = plt.ylim()
                if args.data_type == "cdf":
                    plt.ylim(top=max(cur_max, ymax or np.max(y)), bottom=ymin if ymin is not None else np.min(y))
                else:
                    if cur_min == 0:
                        cur_min = np.min(y)
                    plt.ylim(top=max(cur_max, ymax or np.max(y)), bottom=min(cur_min, ymin if ymin is not None else np.min(y)))
                series = plt.plot(x, y, **extra_args)
                plt.ticklabel_format(axis='both', style='plain')
            case "bar":
                series = plt.bar(x, y)
                plt.ticklabel_format(axis='both', style='plain')
            case "boxplot":
                boxplot_series.append(y)

        if args.dir_labels is not None and plot_type not in {"boxplot", "line"}:
            series.set_label(dir_label_n(filename, args.dir_labels))
        currentHist += 1

    if args.dir_labels is not None:
        if plot_type == "boxplot":
            boxplot_labels = [
                dir_label_n(filename, args.dir_labels) for filename in files
            ]
            plt.boxplot(
                boxplot_series,
                vert=True,
                labels=boxplot_labels,
                showfliers=False,
                showmeans=True,
                showcaps=True,
            )
        else:
            plt.legend(title=args.legend_title)
    fig.set_figheight(args.fig_height)
    fig.set_figwidth(args.fig_width)
    plt.savefig(output_path, pad_inches=0)
