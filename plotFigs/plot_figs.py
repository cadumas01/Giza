# This is a hack to get gus data to plot with gryff data derived from gryff repo.

from folder_to_norm_latencies import extract_norm_latencies
from latencies_to_csv import latencies_to_csv
from csvs_to_plot import data_size_latencies_csvs_to_plot, cdf_csvs_to_plot
import os


def main():
    if os.path.exists("./plotFigs"):
        os.chdir("./plotFigs")
    # Note: folders must be absolute file paths.
    results_folder = "../results/"
    plots_target_folder = "./plots/"


    
    giza_folder = os.path.join(results_folder, "22")
    cassandra_folder = os.path.join(results_folder, "25")
    cholula_folder = os.path.join(results_folder, "21")

# TOdo :Investigate whether order of arguments matters
    #fig 6
    giza_csvs, cassandra_csvs, cholula_csvs = calculate_fig_6_csvs(giza_folder,
                                                                           cassandra_folder,
                                                                           cholula_folder)

    cdf_csvs_to_plot(plots_target_folder, "giza-cassandra-cholula", [giza_csvs, cassandra_csvs, cholula_csvs], ["Giza", "Cassandra", "Cholula"],
                     is_for_reads=False)


    os.unlink(giza_csvs)
    os.unlink(cassandra_csvs)
    os.unlink(cholula_csvs)


# Returns a tuple of tuple of csv paths.
# This is figure 6 in the gryff paper except we display cdf for reads and writes instead of reads and reads in log scale.
def calculate_fig_6_csvs(giza_folder, cassandra_folder, cholula_folder):

    giza_latencies = extract_norm_latencies(giza_folder, is_for_reads=False)
    cholula_latencies = extract_norm_latencies(cholula_folder, is_for_reads=False)
    cassandra_latencies = extract_norm_latencies(cassandra_folder, is_for_reads=False)

    # Calculate csvs for each list of latencies.
    giza_cdf_csv, _ = latencies_to_csv(giza_latencies, "giza", "6a-write")
    cassandra_cdf_csv, _ = latencies_to_csv(cassandra_latencies, "cassandra", "6a-write")
    cholula_cdf_csv, _  = latencies_to_csv(cholula_latencies, "cholula", "6a-write")

    return giza_cdf_csv, cassandra_cdf_csv, cholula_cdf_csv

if __name__ == "__main__":
    main()
