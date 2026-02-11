"""Chart generation tool for the agent."""

from typing import List, Optional

import pandas as pd

from ..charts.engine import ChartEngine
from ..models import ChartType, ChartResult


class ChartTool:
    """
    Tool for generating charts from data.
    
    Wraps the ChartEngine to provide a simple interface
    for the Pydantic AI agent.
    """
    
    def __init__(self, output_format: str = "png") -> None:
        """
        Initialize the chart tool.
        
        Args:
            output_format: Default output format ('png' or 'html')
        """
        self.output_format = output_format
    
    def create_chart(
        self,
        data: List[dict],
        chart_type: str,
        x_column: str,
        y_columns: List[str],
        title: str = "",
        x_label: str = "",
        y_label: str = ""
    ) -> ChartResult:
        """
        Create a chart from data records.
        
        Args:
            data: List of dictionaries (records)
            chart_type: Chart type ('line', 'bar', 'scatter', 'area')
            x_column: Column for x-axis
            y_columns: Column(s) for y-axis
            title: Chart title
            x_label: X-axis label
            y_label: Y-axis label
        
        Returns:
            ChartResult with image or HTML
        """
        df = pd.DataFrame(data)
        
        ct = ChartType(chart_type)
        
        return ChartEngine.create_chart(
            df=df,
            chart_type=ct,
            x_column=x_column,
            y_columns=y_columns,
            title=title,
            x_label=x_label,
            y_label=y_label,
            output_format=self.output_format
        )
