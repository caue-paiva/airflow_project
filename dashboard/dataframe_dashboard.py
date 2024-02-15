import pandas as pd
import plotly.graph_objects as go

# Sample data for bar charts
categories_set = ['Rows in Dataset', 'Hours in Dataset', 'Oldest Date covered', 'Latest Date covered']
values_set_1 = [20, 34, 30, 16]
values_set_2 = [15, 29, 22, 31]

# Replace 'your_file1.csv' and 'your_file2.csv' with the actual paths to your CSV files
csv_file1 = '/home/kap/airflow_test/BTC_DATA (5) copy.csv'
csv_file2 = '/home/kap/airflow_test/BTC_DATA (5).csv'

# Read the CSV files into Pandas DataFrames
df1 = pd.read_csv(csv_file1)
df2 = pd.read_csv(csv_file2)

# Function to create a Plotly bar chart figure
def create_bar_chart(x, y, title):
    fig = go.Figure(data=[
        go.Bar(x=x, y=y, text=y, textposition='outside')
    ])
    fig.update_layout(template='plotly_white', title=title)
    fig.update_xaxes(tickfont=dict(size=18))
    fig.update_yaxes(showticklabels=False)
    fig.update_layout(margin=dict(l=20, r=20, t=70, b=20))
    return fig

# Function to create a Plotly table figure
def create_table_figure(df):
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns), fill_color='paleturquoise', align='left'),
        cells=dict(values=[df[col] for col in df.columns], fill_color='lavender', align='left')
    )])
    fig.update_layout(margin=dict(l=5, r=5, t=5, b=5))
    return fig

# Initialize figure with the first bar chart
fig = create_bar_chart(categories_set, values_set_1, 'Data 1 Statistics')

# Define buttons for interactive update
updatemenus = [
    dict(
        buttons=list([
            dict(label="Data 1 Stats",
                 method="update",
                 args=[{"title": "Data 1 Statistics"},
                       {"data": [go.Bar(x=categories_set, y=values_set_1, text=values_set_1, textposition='outside')]}]),
            dict(label="Data 1 CSV",
                 method="update",
                 args=[{"title": "Data 1 CSV"},
                       {"data": [go.Table(
                           header=dict(values=list(df1.columns), fill_color='paleturquoise', align='left'),
                           cells=dict(values=[df1[col] for col in df1.columns], fill_color='lavender', align='left')
                       )]}]),
            dict(label="Data 2 Stats",
                 method="update",
                 args=[{"title": "Data 2 Statistics"},
                       {"data": [go.Bar(x=categories_set, y=values_set_2, text=values_set_2, textposition='outside')]}]),
            dict(label="Data 2 CSV",
                 method="update",
                 args=[{"title": "Data 2 CSV"},
                       {"data": [go.Table(
                           header=dict(values=list(df2.columns), fill_color='paleturquoise', align='left'),
                           cells=dict(values=[df2[col] for col in df2.columns], fill_color='lavender', align='left')
                       )]}]),
        ]),
        direction="down",
        pad={"r": 10, "t": 10},
        showactive=True,
        x=0.1,
        xanchor="left",
        y=1.1,
        yanchor="top"
    ),
]

# Update the figure layout
fig.update_layout(updatemenus=updatemenus)

# Show the figure
fig.show()
