import csv 

"""
Code for Testing the creating of a simple-front end with HTML/CSS and minimal JS for displaying part of the dataset and its
statistics  
"""

html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Dashboard</title>
    <style>
        
        .dataset-div {{
            display: block; 
            height: 67vh; 
            overflow: auto; 
        }}
        
        #btc {{
            display: block;
        }}

        .content {{
         display: none; 
        }}

        body {{
          font-family: Arial, sans-serif;
        }}

        table {{
        border-collapse: collapse;
        width: 100%;
        border: 1px solid #ddd;
        }}
        th, td {{
            border: 1px solid #ddd;
            text-align: left;
            padding: 8px;
        }}
        th {{
             background-color: #e7f2f8; /* soft blue*/
             color: #333;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}

        tr:nth-child(odd) {{
              background-color: #e7f2f8; /*soft blue */
        }}

        .stats-div {{
            display: none; 
            margin-top: 20px;
        }}

        .bullet-point {{
            background-color: #e7f2f8; 
            padding: 13px;
            border-radius: 5px; 
            list-style-type: none; 
            margin: 0;
            font-size: 19px; 
        }}

        #bold-point {{
            font-weight: bold;
        }}



    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>

<button onclick="showFigure('btc')">BTC DATASET</button>
<button onclick="showFigure('eth')">ETH DATASET</button>

<div id="btc" class="content">
    <div id="btc_df" class="dataset-div">
        {DF_BTC}
    </div>
    <ul class="bullet-points">
        <li class="bullet-point">Dataset Statistics</li>
        <li class="bullet-point">Total rows: {btc_total_rows}</li>
        <li class="bullet-point">Total hours covered: {btc_total_hours}</li>
        <li class="bullet-point">Latest Date covered: {btc_latest_date}</li>
        <li class="bullet-point">Oldest Date covered: {btc_oldest_date}</li>
    </ul>
</div>

<div id="eth" class="content" style="display:none;">
    <div id="eth_df" class="dataset-div">
        {DF_ETH}
    </div>
    <ul class="bullet-points">
        <li class="bullet-point">Dataset Statistics 2222</li>
        <li class="bullet-point">Total rows: {eth_total_rows}</li>
        <li class="bullet-point">Total hours covered: {eth_total_hours}</li>
        <li class="bullet-point">Latest Date covered: {eth_latest_date}</li>
        <li class="bullet-point">Oldest Date covered: {eth_oldest_date}</li>
    </ul>
</div>

<script>
function showFigure(datasetId) {{
    // Hide all content
    var allContent = document.querySelectorAll('.content');
    allContent.forEach(function(content) {{
        content.style.display = 'none';
    }});

    // Show the selected dataset and stats
    var selectedContent = document.getElementById(datasetId);
    if (selectedContent) {{
        selectedContent.style.display = 'block';
    }}
}}
</script>

</body>
</html>
"""

csv_file1 = '/home/kap/airflow_test/BTC_DATA (5) copy.csv'
csv_file2 = '/home/kap/airflow_test/BTC_DATA (5).csv'

categories_set = ['Rows in Dataset', 'Hours in Dataset', 'Oldest Date covered', 'Latest Date covered']
values_set_1 = [20, 34, 30, 16]
values_set_2 = [15, 29, 22, 31]

def csv_to_html_table(csv_filepath):
    with open(csv_filepath, newline='') as csvfile:
        max_rows :int = 500
        reader = csv.reader(csvfile)
        headers = next(reader)
        table = '<table>'
        table += '<tr>' + ''.join([f'<th>{header}</th>' for header in headers]) + '</tr>'
        row_count = 0
        for row in reader:
            table += '<tr>' + ''.join([f'<td>{cell}</td>' for cell in row]) + '</tr>'
            row_count += 1
            if row_count >= max_rows:
                break
        table += '</table>'
        return table


html1 = csv_to_html_table(csv_file1)
html2 = csv_to_html_table(csv_file2)

html_template = html_template.format(
    DF_BTC = html1,
    DF_ETH = html2,
    btc_total_rows = values_set_1[0],
    btc_total_hours = values_set_1[1],
    btc_latest_date = values_set_1[2],
    btc_oldest_date = values_set_1[3],
    eth_total_rows = values_set_1[0]+ 1,
    eth_total_hours = values_set_1[1]+ 1,
    eth_latest_date = values_set_1[2]+ 1,
    eth_oldest_date = values_set_1[3]+ 1,
)

with open("df_templa1.html", "w") as f:
     f.write(html_template)

