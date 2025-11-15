import asyncio
from io import BytesIO
from typing import Optional, get_args

import kaleido  # noqa: F401
import plotly.graph_objects as go
from genjipk_sdk.utilities import DIFFICULTY_COLORS, DIFFICULTY_MIDPOINTS, DIFFICULTY_RANGES_ALL, DifficultyAll


class DifficultyRadarPlotter:
    """A radar style plotter.

    1. Places each difficulty wedge at a numeric angle and colors it.
    2. Draws a black “Votes” polygon and a red average line.
    3. “Masks out” (fills white) exactly the angular span between the lowest
       difficulty visible label and the highest difficulty visible label, but
       only out to the inner radius (max vote radius), leaving the outer rim/grid intact.
    """

    def __init__(self, vote_counts: dict[DifficultyAll, int], min_visible: int = 4) -> None:
        """Initialize DifficultyRadarPlotter.

        Args:
            vote_counts (dict[str, int]): Mapping from difficulty label to vote count.
            min_visible (int): Minimum number of consecutive labels to display.

        """
        self.vote_counts = vote_counts
        self.min_visible = min_visible

    def _determine_visible_labels(self, order: list[DifficultyAll]) -> list[DifficultyAll]:
        """Decide which labels to show, ensuring at least `min_visible` consecutive categories."""
        nonzero_indices = [i for i, lbl in enumerate(order) if self.vote_counts.get(lbl, 0) > 0]
        if not nonzero_indices:
            return []

        start = nonzero_indices[0]
        end = nonzero_indices[-1]
        length = end - start + 1

        if length < self.min_visible:
            deficit = self.min_visible - length
            before = deficit // 2
            after = deficit - before
            start = max(0, start - before)
            end = min(len(order) - 1, end + after)

            length = end - start + 1
            if length < self.min_visible:
                extra = self.min_visible - length
                end = min(len(order) - 1, end + extra)
                length = end - start + 1
            if length < self.min_visible:
                extra = self.min_visible - length
                start = max(0, start - extra)

        return order[start : end + 1]

    def _compute_weighted_average(self, order: list[DifficultyAll]) -> tuple[float, Optional[DifficultyAll]]:
        """Compute the weighted average from midpoint values and map to a label."""
        total_votes = sum(self.vote_counts.values()) or 1
        avg_value = sum(DIFFICULTY_MIDPOINTS[lbl] * self.vote_counts.get(lbl, 0) for lbl in order) / total_votes
        avg_label = self._map_avg_to_label(avg_value)
        return avg_value, avg_label

    @staticmethod
    def _map_avg_to_label(avg: float) -> Optional[DifficultyAll]:
        """Find which difficulty range bucket `avg` falls into."""
        for lbl, (low, high) in DIFFICULTY_RANGES_ALL.items():
            if low <= avg < high:
                return lbl
        return None

    @staticmethod
    def _create_data_trace(angles: list[float], values: list[int]) -> go.Scatterpolar:
        """Build the black “Votes” polygon at numeric angles."""
        return go.Scatterpolar(
            r=values, theta=angles, fill="toself", marker={"color": "black"}, line={"color": "black"}, name="Votes"
        )

    @staticmethod
    def _create_label_trace(
        label_count: int,
        centers: list[float],
        max_r: int,
        visible_labels: list[DifficultyAll],
    ) -> go.Scatterpolar:
        label_trace = go.Scatterpolar(
            r=[max_r * 1.09] * label_count,
            theta=centers,
            mode="text",
            text=visible_labels,
            textfont={"size": 12, "color": "black"},
            hoverinfo="none",
            showlegend=False,
            cliponaxis=False,
        )
        return label_trace

    @staticmethod
    def _create_bar_traces(
        angle_per_label: float,
        centers: list[float],
        max_r: int,
        visible_labels: list[DifficultyAll],
    ) -> list[go.Barpolar]:
        bar_traces = []
        for i, lbl in enumerate(visible_labels):
            bar_traces.append(
                go.Barpolar(
                    r=[max_r],
                    theta=[centers[i]],
                    width=[angle_per_label],
                    marker={"color": DIFFICULTY_COLORS[lbl]},
                    opacity=1.0,
                    hoverinfo="none",
                    showlegend=False,
                )
            )
        return bar_traces

    def plot_difficulty_radar(self) -> go.Figure:
        """Generate and display a radar chart of vote distributions."""
        order = list(get_args(DifficultyAll))
        visible_labels = self._determine_visible_labels(order)
        values = [self.vote_counts.get(lbl, 0) for lbl in visible_labels]
        avg_value, avg_label = self._compute_weighted_average(order)
        visible_labels_count = len(visible_labels)
        max_r = max(values) if values else 0
        angle_per_label = 360.0 / visible_labels_count

        half_span = angle_per_label / 2.0
        sector_start = 90.0 + half_span
        centers: list[float] = [(sector_start + i * angle_per_label) % 360.0 for i in range(visible_labels_count)]

        bar_traces = self._create_bar_traces(angle_per_label, centers, max_r, visible_labels)
        data_trace = self._create_data_trace(centers, values)
        label_trace = self._create_label_trace(visible_labels_count, centers, max_r, visible_labels)

        fig = go.Figure(data=[*bar_traces, data_trace, label_trace])
        fig.update_layout(
            polar={
                "bgcolor": "rgba(0,0,0,0)",
                "angularaxis": {"visible": False},
                "radialaxis": {
                    "visible": True,
                    "tickmode": "linear",
                    "dtick": 1,
                    "tick0": 0,
                    "tickformat": "d",
                    "layer": "above traces",
                },
                "sector": [-270 + (angle_per_label * (2 / 9)), 90 - (angle_per_label * (2 / 9))],
            },
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            title=f"Vote Distribution (avg = {avg_value:.2f} → {avg_label})",
            width=1400,
            height=700,
        )

        for r in range(1, max_r + 1):
            fig.add_trace(
                go.Scatterpolar(
                    r=[r] * 361,
                    theta=list(range(360)),
                    mode="lines",
                    line={"color": "black", "width": 1},
                    hoverinfo="none",
                    showlegend=False,
                )
            )

        return fig


async def build_playtest_plot(vote_counts: dict[DifficultyAll, int]) -> BytesIO:
    """Build a playtest plot."""
    plotter = DifficultyRadarPlotter(vote_counts, min_visible=6)
    fig = await asyncio.to_thread(plotter.plot_difficulty_radar)
    buffer = BytesIO()
    fig.write_image(file=buffer, format="png")
    buffer.seek(0)
    return buffer
