import pandas as pd


def params_to_html(parameters: dict) -> str:
    p_df = pd.DataFrame(
        parameters.items(),
        columns = ["Parameter", "Value"]
    )
    p_df.set_index("Parameter", inplace=True)
    p_html = p_df.to_html(border=1)

    return p_html


def stat_to_html(stats: pd.Series) -> str:
    rdf = stats.to_frame(name="Value")
    rdf.index.name = "Metric"
    rdf = rdf[list(map(lambda x: not x in ["_equity_curve", "_trades"], rdf.index))]
    r_html = rdf.to_html(border=1)

    return r_html


def html_report(
    parameters: dict,
    stats: pd.Series,
    header: str = "Strategy Backtesting Results"
) -> str:
    p_html = params_to_html(parameters=parameters)
    r_html = stat_to_html(stats=stats)
    html_content = f"<h3>{header}</h3>" +\
        "<p><b>Parameters</b></p>" + p_html +\
        "<p><b>Metrics</b></p>" + r_html
    
    return html_content