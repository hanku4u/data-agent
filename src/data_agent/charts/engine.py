"""Plotly-based chart generation engine."""

import base64
from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go

from ..models import ChartType, ChartResult


class ChartEngine:
    """Generate charts from DataFrames using Plotly."""
    
    @staticmethod
    def create_chart(
        df: pd.DataFrame,
        chart_type: ChartType,
        x_column: str,
        y_columns: List[str],
        title: str = "",
        x_label: str = "",
        y_label: str = "",
        output_format: str = "png"
    ) -> ChartResult:
        """
        Create a chart from a DataFrame.
        
        Args:
            df: Source data
            chart_type: Type of chart to create
            x_column: Column for x-axis
            y_columns: Column(s) for y-axis
            title: Chart title
            x_label: X-axis label
            y_label: Y-axis label
            output_format: 'png' or 'html'
        
        Returns:
            ChartResult with base64 image or HTML
        """
        # Work on a copy to avoid mutating the input
        df = df.copy()
        
        # Auto-detect and parse datetime x-axis
        if x_column in df.columns:
            try:
                df[x_column] = pd.to_datetime(df[x_column])
            except (ValueError, TypeError):
                pass
        
        # Generate title if not provided
        if not title:
            y_names = ", ".join(y_columns)
            title = f"{y_names} over {x_column}"
        
        # Create the chart
        fig = ChartEngine._build_figure(df, chart_type, x_column, y_columns, title)
        
        # Style the chart
        fig.update_layout(
            title=dict(text=title, x=0.5),
            xaxis_title=x_label or x_column,
            yaxis_title=y_label or (y_columns[0] if len(y_columns) == 1 else ""),
            template="plotly_white",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=60, r=30, t=80, b=60),
            width=900,
            height=500
        )
        
        # Format datetime x-axis
        if pd.api.types.is_datetime64_any_dtype(df[x_column]):
            fig.update_xaxes(
                tickformat="%Y-%m-%d",
                tickangle=45
            )
        
        # Generate output
        result = ChartResult(
            chart_type=chart_type.value,
            title=title,
            data_points=len(df)
        )
        
        if output_format == "html":
            result.html = fig.to_html(include_plotlyjs="cdn", full_html=False)
        else:
            # Generate PNG as base64
            img_bytes = fig.to_image(format="png", scale=2)
            result.image_base64 = base64.b64encode(img_bytes).decode("utf-8")
        
        return result
    
    @staticmethod
    def _build_figure(
        df: pd.DataFrame,
        chart_type: ChartType,
        x_column: str,
        y_columns: List[str],
        title: str
    ) -> go.Figure:
        """Build the Plotly figure based on chart type."""
        
        fig = go.Figure()
        
        if chart_type == ChartType.LINE:
            for y_col in y_columns:
                fig.add_trace(go.Scatter(
                    x=df[x_column],
                    y=df[y_col],
                    mode="lines+markers",
                    name=y_col,
                    line=dict(width=2),
                    marker=dict(size=4)
                ))
        
        elif chart_type == ChartType.BAR:
            for y_col in y_columns:
                fig.add_trace(go.Bar(
                    x=df[x_column],
                    y=df[y_col],
                    name=y_col
                ))
            if len(y_columns) > 1:
                fig.update_layout(barmode="group")
        
        elif chart_type == ChartType.SCATTER:
            for y_col in y_columns:
                fig.add_trace(go.Scatter(
                    x=df[x_column],
                    y=df[y_col],
                    mode="markers",
                    name=y_col,
                    marker=dict(size=8, opacity=0.7)
                ))
        
        elif chart_type == ChartType.AREA:
            for y_col in y_columns:
                fig.add_trace(go.Scatter(
                    x=df[x_column],
                    y=df[y_col],
                    fill="tonexty" if y_columns.index(y_col) > 0 else "tozeroy",
                    name=y_col,
                    line=dict(width=1)
                ))
        
        return fig
