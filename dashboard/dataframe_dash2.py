import pandas as pd
import plotly.graph_objects as go

# Sample data for bar charts
html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Dashboard</title>
    <style>
        /* Style to make the DataFrame divs scrollable */
        .dataset-div {{
            display: none; /* Initially hide divs */
            height: 90vh; /* Set a fixed height for the divs */
            overflow: auto; /* Enable scrolling within the div */
        }}
        
        /* Make the first DataFrame div visible by default */
        #btc_df {{
            display: block;
        }}
    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>

<button onclick="showFigure('btc_df')">BTC DATASET</button>
<button onclick="showFigure('eth_df')">ETH DATASET</button>
<button onclick="showFigure('btc_plt')">BTC DATASET STATS</button>
<button onclick="showFigure('eth_plt')">ETH DATASET STATS</button>

<div id="btc_df" class="dataset-div">
    {DF_BTC}
</div>

<div id="eth_df" class="dataset-div">
   {DF_ETH}
</div>

<div id="btc_plt" class="dataset-div">
   {PLT_BTC}
</div>

<div id="eth_plt" class="dataset-div">
   {PLT_ETH}
</div>

<!-- More divs for other DataFrames -->

<script>
function showFigure(figId) {{
    // Hide all dataset divs
    var allDivs = document.querySelectorAll('.dataset-div');
    allDivs.forEach(function(div) {{
        div.style.display = 'none';
    }});

    // Show the selected dataset div
    var selectedDiv = document.getElementById(figId);
    if (selectedDiv) {{
        selectedDiv.style.display = 'block';
    }}
}}
</script>

</body>
</html>
"""


# Replace 'your_file1.csv' and 'your_file2.csv' with the actual paths to your CSV files
csv_file1 = '/home/kap/airflow_test/BTC_DATA (5) copy.csv'
csv_file2 = '/home/kap/airflow_test/BTC_DATA (5).csv'

categories_set = ['Rows in Dataset', 'Hours in Dataset', 'Oldest Date covered', 'Latest Date covered']
values_set_1 = [20, 34, 30, 16]

values_set_2 = [15, 29, 22, 31]


# Read the CSV files into Pandas DataFrames
df1 = pd.read_csv(csv_file1)
df2 = pd.read_csv(csv_file2)

# Function to create a Plotly bar chart figure

# Function to create a Plotly table figure
def create_table_figure(df):
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns), fill_color='paleturquoise', align='left'),
        cells=dict(values=[df[col] for col in df.columns], fill_color='lavender', align='left'),
       
    )])
    fig.update_layout(margin=dict(l=5, r=5, t=5, b=5), height = 700, width = 1700)
    return fig

def create_plot_fig(values: list, categories: list[str]):
    fig = go.Figure(data=[
    go.Bar(x= categories, y=values, name="Set",
           text=values_set_1, textposition='outside')
    ])

    fig.update_xaxes(tickfont=dict(size=18))
    fig.update_layout(template='plotly_white', title='Crypto Data Dashboard')
    fig.update_layout(margin=dict(l=20, r=20, t=70, b=20), height = 700, width = 1700)
    fig.update_yaxes(showticklabels=False)

    return fig 



fig1 = create_table_figure(df1)
fig2 = create_table_figure(df2)

fig3 = create_plot_fig(values_set_1, categories_set)
fig4 = create_plot_fig(values_set_2, categories_set)

html1 = fig1.to_html(full_html = False, include_plotlyjs=False)
html2 = fig2.to_html(full_html = False, include_plotlyjs=False )
html3 = fig3.to_html(full_html = False, include_plotlyjs=False )
html4 = fig4.to_html(full_html = False, include_plotlyjs=False )


html_template = html_template.format(DF_BTC = html1,DF_ETH = html2,PLT_BTC = html3, PLT_ETH = html4 )

with open("df_templa1.html", "w") as f:
     f.write(html_template)

#with open("df2.html", "w") as f:
   #  f.write(html2)