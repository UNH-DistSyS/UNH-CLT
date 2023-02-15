import os
import pandas as pd
import matplotlib.pyplot as plt

# Define the folder path and file extension
folder_path = '/path/to/folder'
file_extension = '.csv'

# Create a list of all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith(file_extension)]

# Iterate over each CSV file and plot a histogram
for csv_file in csv_files:
    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(os.path.join(folder_path, csv_file))

    # Extract the column with the data you want to plot
    data_column = 'Count'
    data = df[data_column]

    # Plot a histogram of the data using Matplotlib
    plt.hist(data, bins=10)
    plt.title(f'Histogram of {data_column} in {csv_file}')
    plt.xlabel(data_column)
    plt.ylabel('Latency')
    plt.show()

# Calculate statistics
mean = np.mean(data)
std = np.std(data)
var = np.var(data)

print(f"Mean: {mean}")
print(f"Standard deviation: {std}")
print(f"Variance: {var}")