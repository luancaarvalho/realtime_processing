import argparse
import json
import pandas as pd
import plotly.express as px


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="aula01/demo05/demo5_data.json")
    parser.add_argument(
        "--output-html", default="aula01/demo05/demo5_plot_plotly_left.html"
    )
    parser.add_argument(
        "--output-png", default="aula01/demo05/demo5_plot_plotly_left.png"
    )
    parser.add_argument(
        "--logo",
        default="aula01/demo05/unifor_logo.png",
        help="path to logo image (optional)",
    )
    args = parser.parse_args()

    with open(args.input, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df["event_dt"] = pd.to_datetime(df["event_time_ms"], unit="ms")
    df["proc_dt"] = pd.to_datetime(df["processing_time_ms"], unit="ms")

    color_map = {"green": "#2ca02c", "orange": "#ff7f0e", "red": "#d62728"}
    blue = "#0066CC"

    fig = px.scatter(
        df,
        x="proc_dt",
        y="event_dt",
        color="color",
        color_discrete_map=color_map,
        hover_data=["event_time_ms", "processing_time_ms", "color"],
        labels={"proc_dt": "Processing Time", "event_dt": "Event Time"},
        opacity=0.85,
        width=1200,
        height=800,
    )

    tmin = min(df["proc_dt"].min(), df["event_dt"].min())
    tmax = max(df["proc_dt"].max(), df["event_dt"].max())
    fig.add_shape(
        type="line",
        xref="x",
        yref="y",
        x0=tmin,
        y0=tmin,
        x1=tmax,
        y1=tmax,
        line=dict(dash="dash", color="black"),
    )

    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="#E5ECF6",
        margin=dict(l=80, r=80, t=120, b=60),
    )

    import os, base64

    images = []
    annotations = []

    annotations.append(
        dict(
            x=0.18,
            y=1.13,
            xref="paper",
            yref="paper",
            showarrow=False,
            text="<b>Engenharia de Dados - Processamento em Tempo Real</b>",
            font=dict(size=20, color=blue),
            align="left",
        )
    )

    logo_path = args.logo
    if logo_path and os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode()
            images.append(
                dict(
                    source="data:image/png;base64," + encoded,
                    xref="paper",
                    yref="paper",
                    x=0.02,
                    y=1.11,
                    sizex=0.10,
                    sizey=0.10,
                    xanchor="left",
                    yanchor="middle",
                    opacity=0.95,
                    layer="above",
                )
            )
        except Exception:
            print(f"Warning: could not load logo at {logo_path}")

    if images:
        fig.update_layout(images=images)
    if annotations:
        fig.update_layout(annotations=annotations)

    fig.write_html(args.output_html)
    try:
        fig.write_image(args.output_png, scale=2)
    except Exception:
        print("PNG export failed (kaleido missing), but HTML was saved.")

    print(f"Saved HTML to {args.output_html}")
    print(f"Saved PNG to {args.output_png} (if kaleido available)")


if __name__ == "__main__":
    main()
