import pandas as pd
import plotly.graph_objects as go

# Replace 'your_file.csv' with the path to your CSV file
csv_file1 = '/home/kap/airflow_test/BTC_DATA (5).csv'
csv_file2 = "/home/kap/airflow_test/debug.csv"

# Read the CSV file into a Pandas DataFrame
df1 = pd.read_csv(csv_file1)
print(df1.columns)
df2 = pd.read_csv(csv_file2)



fig = go.Figure(data=[go.Table(
    header=dict(values=list(df1.columns),
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=[df1[col] for col in df1.columns],
               fill_color='lavender',
               align='left'))
])

# Update layout for a more compact display style if desired
fig.update_layout(margin=dict(l=5, r=5, t=5, b=5))

# Show the figure
fig.show()