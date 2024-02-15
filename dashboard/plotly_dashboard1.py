import plotly.graph_objects as go
x0, x1, y0, y1 = 0.0, 1, 0.0, 1

# Create a blank figure
fig = go.Figure()

# Calculate the rectangle's position with 15% offset


# Add a rectangle shape for the box, making it dark blue
fig.add_shape(type="rect",
              x0=x0, y0=y0, x1=x1, y1=y1,
               # White border for contrast
              fillcolor="#3136aa",  # A dark blue color similar to the image
              xref="paper", yref="paper",
              layer="below")

# Add annotations for title and subtitles with numbers inside the box
fig.add_annotation(x=0.5, y=1.1, xref="paper", yref="paper",
                   text="CRYPTO DATASET DASHBOARD", showarrow=False,
                   font=dict(family="Arial, sans-serif", size=30, color="#00FFFF"))

# Subtitles and numbers (example placeholders)
# Subtitles and numbers (example placeholders)
tokens_colors = {
    "Bitcoin": "#FFFF00",
    "Ethereum": "#00FF00",
    "Solana":  "#FF6600"
}
subtitles = ["Bitcoin", "Ethereum"]

token_data: dict[str, str | int | float] = {
    "Dataset Rows" : 0,
    "Dataset Hours" : 0.0,
    "Oldest Date covered" : "",
    "Latest Date covered" : "",
}

# Calculate the x positions for the subtitles
num_subtitles = len(subtitles)
margin_ratio = 0.10  # 15% margin on each side
space_between_subtitles = (1.0 - 5 * margin_ratio) / (num_subtitles - 1)

# Create the subtitles, evenly spaced out across the plot
for i, token in enumerate(subtitles):
    subtitle_x = margin_ratio + i * space_between_subtitles
    fig.add_annotation(x=subtitle_x, y=0.9, xref="paper", yref="paper",
                       text=f"<b>{token}</b>", showarrow=False,
                       font=dict(family="Arial, sans-serif", size=28, color="white"),
                       xanchor="left")

    # Add bullet points for each subtitle
    bullet_points = list(token_data.keys())
    for j, point in enumerate(bullet_points):
        fig.add_annotation(x=subtitle_x, y=0.85 - (j+1)*0.15, xref="paper", yref="paper",
                           text=f"• {point}: {token_data[point]}", showarrow=False,
                           font=dict(family="Arial, sans-serif", size=22, color="white"),
                           xanchor="left")
# Change the figure background to a dark color and set the font
fig.update_layout(xaxis=dict(showgrid=False, zeroline=False, visible=False),
                  yaxis=dict(showgrid=False, zeroline=False, visible=False),
                  plot_bgcolor="#1e2130",  # A dark background color similar to the image
                  paper_bgcolor="#1e2130",  # Same as plot background color
                  font=dict(family="Arial, sans-serif", color="white"),  # Set font and text color
                  showlegend=False,
                 
                 
                  )
html_string = fig.to_html()

with open("plotly_output.html", "w") as f:
     f.write(html_string)