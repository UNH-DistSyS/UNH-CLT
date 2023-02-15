import json
import csv
import numpy as np

def create_histogram(csv_files, experimental_buckets, num_bins):
    # Load experimental buckets
    with open(experimental_buckets, 'r') as f:
        buckets = json.load(f)['buckets']

    # Create histogram bins
    bins = np.linspace(0, num_bins, num_bins + 1)

    # Initialize histograms for each bucket
    histograms = {}
    for bucket in buckets:
        histograms[bucket['label']] = np.zeros(num_bins)

    # Process CSV files
    for csv_file in csv_files:
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip header row

            # Process each row in the CSV file
            for row in reader:
                sender = float(row[0])
                receiver = float(row[1])
                latency = float(row[2])

                # Find the corresponding bucket for this row
                bucket = None
                for b in buckets:
                    if [sender, receiver] in b['node_pairs'] or [receiver, sender] in b['node_pairs']:
                        bucket = b['label']
                        break

                # Add latency to the appropriate bin in the histogram
                if bucket is not None:
                    idx = np.digitize(latency, bins)
                    histograms[bucket][idx - 1] += 1

    # Save histograms to CSV files
    for label, histogram in histograms.items():
        with open(f'{label}_histogram.csv', 'w') as f:
            writer = csv.writer(f)
            for i, count in enumerate(histogram):
                writer.writerow([bins[i], count])

    print('Histograms saved to CSV files.')

# Example usage
csv_files = ['file1.csv', 'file2.csv', 'file3.csv']
experimental_buckets = 'buckets.json'
num_bins = 10
create_histogram(csv_files, experimental_buckets, num_bins)
