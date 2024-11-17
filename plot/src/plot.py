import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import os

# File paths
LOG_DIR = "../../logs"
LOG_FILE = os.path.join(LOG_DIR, "metric_log.csv")
OUTPUT_FILE = os.path.join(LOG_DIR, "error_distribution.png")

print("Starting plot service...")

while True:
    # Check if the log file exists
    if os.path.exists(LOG_FILE):
        try:
            # Read the CSV file
            data = pd.read_csv(LOG_FILE)

            if "absolute_error" in data.columns:
                # Generate the histogram with KDE
                plt.figure(figsize=(8, 6))
                sns.histplot(data["absolute_error"], bins=15, kde=True, color="orange", edgecolor='black', alpha=0.7)
                plt.title("Error Distribution with KDE", fontsize=16)
                plt.xlabel("Absolute Error", fontsize=14)
                plt.ylabel("Frequency", fontsize=14)
                plt.grid(axis='y', linestyle='--', alpha=0.7)

                # Save the histogram to a file
                plt.savefig(OUTPUT_FILE)
                plt.close()

                print(f"Updated histogram with KDE saved to {OUTPUT_FILE}")
            else:
                print("No 'absolute_error' column found in the log file.")
        except pd.errors.EmptyDataError:
            print(f"The log file {LOG_FILE} is empty. Waiting for data...")
        except Exception as e:
            print(f"Error processing the log file: {e}")
    else:
        print(f"Log file {LOG_FILE} not found. Waiting for it to appear...")

    # Wait for 10 seconds before the next read
    time.sleep(10)
