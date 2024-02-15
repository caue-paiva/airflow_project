import plotly.graph_objects as go

# Sample data sets
categories_set = ['Rows in Dataset', 'Hours in Dataset', 'Oldest Date covered', 'Latest Date covered']
values_set_1 = [20, 34, 30, 16]

values_set_2 = [15, 29, 22, 31]

# Initial bar chart using the first set of data
fig = go.Figure(data=[
    go.Bar(x=categories_set, y=values_set_1, name="Set 1",
           text=values_set_1, textposition='outside')
])
# Make the category text larger
fig.update_xaxes(tickfont=dict(size=18))


# Apply the 'plotly_dark' template
fig.update_layout(template='plotly_white', title='Crypto Data Dashboard')

# Add buttons for interactive update with better positioning
fig.update_layout(
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            x=0.5,
            y=1.15,
            xanchor='center',
            yanchor='top',
            showactive=False,
            buttons=list([
                dict(
                    args=[{"x": [categories_set], "y": [values_set_1], 
                           "text": [values_set_1]}],
                    label="BTC",
                    method="update"
                ),
                dict(
                    args=[{"x": [categories_set], "y": [values_set_2],
                           "text": [values_set_2]}],
                    label="ETH",
                    method="update"
                )
            ]),
            font=dict(size=12), # Adjust the size as needed
        )
    ]
)


# Adjust the margins to ensure the buttons are not cut off
fig.update_layout(margin=dict(l=20, r=20, t=70, b=20))

# Remove Y-axis ticks and labels
fig.update_yaxes(showticklabels=False)

# Show the figure
fig.show()
