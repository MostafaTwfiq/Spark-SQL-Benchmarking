import matplotlib.pyplot as plt

class MetricsPlotter:

    def plot_benchmark_results(self, queries, tables_metrics, metric_type, title):
        # Calculate dynamic group width
        num_formats = len(tables_metrics)
        group_width = 0.8 / num_formats  

        index = range(len(queries))

        fig, ax = plt.subplots()
        for group_index, (table_format, metrics) in enumerate(tables_metrics.items()):
            group_offset = group_width * group_index
            ax.bar([i + group_offset for i in index], metrics, group_width, label=table_format)

        # Customize the plot
        ax.set_xlabel('Queries')
        ax.set_ylabel(f'{metric_type}')
        ax.set_title(title)
        ax.set_xticks([i + 0.4 for i in index])  
        ax.set_xticklabels(queries)
        ax.legend()

        # Save the plot as a PNG file
        plt.savefig(f'{title}.png')



if __name__ == '__main__':

    # Sample data
    categories = ['Category 1', 'Category 2', 'Category 3', 'Category 4']
    hive_durations = [100, 150, 120, 200]  # Replace with actual durations for Hive
    iceberg_durations = [80, 130, 110, 180]  # Replace with actual durations for Iceberg
    impala_durations = [50, 100, 150, 10]  # Replace with actual durations for Iceberg

    plotter = MetricsPlotter()
    plotter.plot_benchmark_results(queries=categories, 
                                   tables_metrics={'hive': hive_durations, 'iceberg': iceberg_durations, 'impala':impala_durations},
                                   metric_type='duration', title='Hive vs Iceberg vs Impala')
