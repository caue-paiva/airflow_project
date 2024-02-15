import plotly.graph_objects as go

# Sample data sets
categories_set_1 = ['Category A', 'Category B', 'Category C', 'Category D']
values_set_1 = [20, 34, 30, 16]

categories_set_2 = ['Category E', 'Category F', 'Category G', 'Category H']
values_set_2 = [15, 29, 22, 31]

# Initial bar chart using the first set of data
fig = go.Figure(data=[go.Bar(x=categories_set_1, y=values_set_1, name="Set 1")])

# Apply the 'plotly_dark' template
fig.update_layout(template='plotly_white', title='Interactive Bar Chart with Plotly')

# Add buttons for interactive update
fig.update_layout(
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            x=0.7,
            y=1.2,
            buttons=list([
                dict(
                    args=[{"x": [categories_set_1], "y": [values_set_1]}],
                    label="Set 1",
                    method="update"
                ),
                dict(
                    args=[{"x": [categories_set_2], "y": [values_set_2]}],
                    label="Set 2",
                    method="update"
                )
            ]),
        )
    ]
)

# Show the figure
fig.show()

html_string = fig.to_html()

with open("plotly_output2.html", "w") as f:
     f.write(html_string)