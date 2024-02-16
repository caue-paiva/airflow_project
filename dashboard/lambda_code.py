import csv
from io import StringIO

def csv_string_to_html_table(csv_string:str, max_rows:int = 300)->str:
    csvfile = StringIO(csv_string)
    reader = csv.reader(csvfile)
    headers = next(reader)

    table = "<table>"
    table += '<tr>' + ''.join([f'<th>{header}</th>' for header in headers]) + '</tr>'
    row_count = 0
    for row in reader:
        table += '<tr>' + ''.join([f'<td>{cell}</td>' for cell in row]) + '</tr>'
        row_count += 1
        if row_count >= max_rows:
            break
    table += '</table>'
    return table




test_str = """
DATE,BTCUSDT_START_PRICE,BTCUSDT_END_PRICE,BTCUSDT_COINS_BOUGHT,BTCUSDT_COINS_SOLD,BTCUSDT_NET_FLOW,BTCUSDT_TOTAL_AGGRT_TRANSACTIONS\n2024-02-14 12:17:55,51572.39,51365.04,0.186,0.699,-26358.215,171\n2024-02-14 12:12:55,51826.15,51572.39,0.583,1.93,-69449.166,241\n2024-02-14 12:07:55,51896.14,51826.15,0.02,0.144,-6429.691,63\n2024-02-14 12:02:55,51778.49,51896.14,0.148,0.058,4675.808,101\n2024-02-14 11:57:55,51925.0,51778.49,0.073,0.197,-6434.319,62\n2024-02-14 11:52:55,51752.95,51925.0,0.75,0.504,12700.143,117\n2024-02-14 11:47:55,51778.49,51752.95,0.095,0.104,-430.566,61\n2024-02-14 11:42:55,51957.1,51778.49,0.128,0.968,-43513.018,165\n2024-02-14 11:37:55,51863.74,51957.1,3.856,0.867,155422.47,306\n2024-02-14 11:32:55,51614.32,51863.74,1.26,2.514,-64452.598,373\n2024-02-14 11:27:55,51592.8,51614.32,0.197,0.01,9671.33,58
"""
html_table = csv_string_to_html_table(test_str)


with open("test_html.html", "w") as f:
    f.write(html_table)


def csv_to_html_table2(csv_filepath):
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

csv_file1 = '/home/kap/airflow_test/BTC_DATA (5) copy.csv'

html2 = csv_to_html_table2(csv_file1)

with open("test_html2.html", "w") as f:
    f.write(html2)