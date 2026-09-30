"""
Rich-powered terminal visualizations and analysis views.

Theme: clean retro - heavy use of yellow on black/dark.
Tables are designed to be scannable and informative for a sovereign operator.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.style import Style
from rich.table import Table
from rich.text import Text

from .database import Database
from .models import Node

# Retro yellow-on-black theme
YELLOW = Style(color="yellow", bold=False)
YELLOW_BOLD = Style(color="yellow", bold=True)
DIM = Style(color="bright_black")
HEADER = Style(color="yellow", bold=True)
GREEN = Style(color="green")
RED = Style(color="red")
CYAN = Style(color="cyan")


console = Console(theme=None, style=Style(color="yellow"))


def _yellow_table(title: str) -> Table:
    t = Table(
        title=title,
        title_style=YELLOW_BOLD,
        border_style=Style(color="yellow"),
        header_style=HEADER,
        show_lines=False,
        expand=False,
    )
    return t


def render_global_stats(stats: Dict[str, Any], geo_stats: Optional[Dict[str, Any]] = None) -> None:
    """Main overview panel."""
    total = stats.get("total_nodes", 0)
    knots = stats.get("knots_nodes", 0)
    listening = stats.get("listening_nodes", 0)
    crawled = stats.get("crawled_at_least_once", 0)
    max_h = stats.get("max_height", 0)
    avg_lat = stats.get("avg_latency_ms")

    knots_pct = (knots / total * 100) if total else 0
    listen_pct = (listening / total * 100) if total else 0

    grid = Table.grid(expand=True)
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)

    left = Text()
    left.append("Total nodes discovered: ", style=DIM)
    left.append(f"{total:,}\n", style=YELLOW_BOLD)
    left.append("Knots nodes:           ", style=DIM)
    left.append(f"{knots:,} ", style=YELLOW_BOLD)
    left.append(f"({knots_pct:.1f}%)\n", style=DIM)
    left.append("Listening (NODE_NET):  ", style=DIM)
    left.append(f"{listening:,} ", style=YELLOW_BOLD)
    left.append(f"({listen_pct:.1f}%)\n", style=DIM)
    left.append("Successfully crawled:  ", style=DIM)
    left.append(f"{crawled:,}\n", style=YELLOW)

    right = Text()
    right.append("Best chain height seen: ", style=DIM)
    right.append(f"{max_h:,}\n", style=YELLOW_BOLD)
    if avg_lat:
        right.append("Avg latency (crawled):  ", style=DIM)
        right.append(f"{avg_lat:.1f} ms\n", style=YELLOW)
    right.append("GeoIP available:        ", style=DIM)
    right.append("YES" if (geo_stats and geo_stats.get("by_country")) else "NO / partial\n", style=GREEN if (geo_stats and geo_stats.get("by_country")) else RED)

    console.print(
        Panel(
            Group(left, Text(), right),
            title="[yellow]knots-network-crawler :: GLOBAL STATS[/yellow]",
            border_style="yellow",
            padding=(1, 2),
        )
    )

    # Version distribution
    if stats.get("version_distribution"):
        t = _yellow_table("Version / User Agent distribution (top)")
        t.add_column("User Agent / Subversion", style=YELLOW)
        t.add_column("Count", justify="right", style=YELLOW_BOLD)
        t.add_column("%", justify="right", style=DIM)
        total_v = sum(c for _, c in stats["version_distribution"])
        for ua, count in stats["version_distribution"]:
            pct = (count / total_v * 100) if total_v else 0
            t.add_row(ua[:72], f"{count:,}", f"{pct:.1f}%")
        console.print(t)

    # Top countries
    if stats.get("top_countries"):
        t = _yellow_table("Geographic distribution (top countries)")
        t.add_column("Country", style=YELLOW)
        t.add_column("Nodes", justify="right", style=YELLOW_BOLD)
        for country, count in stats["top_countries"][:12]:
            t.add_row(country or "Unknown", f"{count:,}")
        console.print(t)


def render_geo_breakdown(geo: Dict[str, Any]) -> None:
    """Detailed country + ASN view."""
    by_country = geo.get("by_country", [])
    by_asn = geo.get("by_asn", [])

    if by_country:
        t = _yellow_table("Nodes by Country (full)")
        t.add_column("Country", style=YELLOW)
        t.add_column("Code", style=CYAN)
        t.add_column("Count", justify="right", style=YELLOW_BOLD)
        for row in by_country[:30]:
            t.add_row(row.get("country") or "?", row.get("country_code") or "", str(row.get("count", 0)))
        console.print(t)

    if by_asn:
        t = _yellow_table("Top ASNs / Hosting Providers")
        t.add_column("ASN / Org", style=YELLOW)
        t.add_column("Count", justify="right", style=YELLOW_BOLD)
        for row in by_asn[:20]:
            label = f"AS{row.get('asn')} - {row.get('asn_org')}" if row.get("asn") else "?"
            t.add_row(label, str(row.get("count", 0)))
        console.print(t)


def render_node_table(nodes: List[Node], title: str = "Nodes", max_rows: int = 30) -> None:
    """Generic rich node table."""
    if not nodes:
        console.print(Panel(f"[yellow]No nodes in this view.[/yellow]", border_style="yellow"))
        return

    t = _yellow_table(title)
    t.add_column("IP:Port", style=YELLOW, no_wrap=True)
    t.add_column("Height", justify="right")
    t.add_column("Δ", justify="right", style=DIM)
    t.add_column("Knots?", justify="center")
    t.add_column("Listen?", justify="center")
    t.add_column("Latency", justify="right")
    t.add_column("Country / ASN", style=CYAN, no_wrap=True)
    t.add_column("Last Seen", style=DIM)

    for n in nodes[:max_rows]:
        height = f"{n.start_height:,}" if n.start_height else "?"
        delta = f"{n.last_height_delta:+d}" if n.last_height_delta is not None else ""
        knots = "✓" if n.is_knots else ""
        listen = "✓" if n.services_listening else ""
        lat = f"{n.latency_ms:.0f}ms" if n.latency_ms else ""
        geo = ""
        if n.geo.country:
            geo = n.geo.country[:14]
        if n.geo.asn:
            geo += f" AS{n.geo.asn}"
        last = n.last_seen[:16].replace("T", " ") if n.last_seen else ""

        t.add_row(
            f"{n.ip}:{n.port}",
            height,
            delta,
            Text(knots, style=GREEN if n.is_knots else DIM),
            Text(listen, style=GREEN if n.services_listening else DIM),
            lat,
            geo or "?",
            last,
        )
    console.print(t)


def render_knots_view(db: Database, limit: int = 60) -> None:
    """Dedicated Knots nodes view."""
    # We fetch inside because we want fresh data
    # (caller can pass already fetched if wanted)
    import asyncio

    async def _inner():
        nodes = await db.get_knots_nodes(limit=limit)
        render_node_table(nodes, title=f"Bitcoin Knots nodes ({len(nodes)} shown)", max_rows=limit)

    try:
        asyncio.get_event_loop().run_until_complete(_inner())
    except RuntimeError:
        # Already inside loop (from CLI)
        loop = asyncio.get_event_loop()
        loop.create_task(_inner())  # fire and forget not ideal, but CLI will call sync wrappers mostly


def render_listening_view(db: Database, limit: int = 60) -> None:
    async def _inner():
        nodes = await db.get_listening_nodes(limit=limit)
        render_node_table(nodes, title=f"Reachable / Listening nodes (NODE_NETWORK) - {len(nodes)} shown", max_rows=limit)

    try:
        asyncio.get_event_loop().run_until_complete(_inner())
    except RuntimeError:
        pass


def render_top_height_view(db: Database, limit: int = 40) -> None:
    async def _inner():
        nodes = await db.get_top_by_height(limit=limit)
        render_node_table(nodes, title=f"Nodes reporting highest block height (top {limit})", max_rows=limit)

    try:
        asyncio.get_event_loop().run_until_complete(_inner())
    except RuntimeError:
        pass


async def render_live_progress(stats: CrawlStats, cfg: Any, console: Console) -> None:
    """Live updating panel used during crawl (called from CLI)."""
    # This is called periodically from the progress callback in a Live context.
    # We just print a compact line; the Live context in cli.py handles refresh.
    elapsed = stats.elapsed
    rate = stats.succeeded / elapsed if elapsed > 0 else 0

    line = (
        f"[yellow]crawled[/yellow] {stats.succeeded}  "
        f"[yellow]discovered[/yellow] {stats.discovered}  "
        f"[yellow]knots[/yellow] {stats.knots_found}  "
        f"[yellow]listening[/yellow] {stats.listening_found}  "
        f"[yellow]maxh[/yellow] {stats.max_height:,}  "
        f"rate {rate:.1f}/s  elapsed {elapsed:.0f}s  queue ~{getattr(stats, 'qsize', '?')}"
    )
    console.print(line, highlight=False)


def print_banner() -> None:
    banner = """
[yellow]██╗  ██╗███╗   ██╗ ██████╗ ████████╗███████╗     ██████╗██████╗  █████╗ ██╗    ██╗██╗     ███████╗██████╗ [/yellow]
[yellow]██║ ██╔╝████╗  ██║██╔═══██╗╚══██╔══╝██╔════╝    ██╔════╝██╔══██╗██╔══██╗██║    ██║██║     ██╔════╝██╔══██╗[/yellow]
[yellow]█████╔╝ ██╔██╗ ██║██║   ██║   ██║   ███████╗    ██║     ██████╔╝███████║██║ █╗ ██║██║     █████╗  ██████╔╝[/yellow]
[yellow]██╔═██╗ ██║╚██╗██║██║   ██║   ██║   ╚════██║    ██║     ██╔══██╗██╔══██║██║███╗██║██║     ██╔══╝  ██╔══██╗[/yellow]
[yellow]██║  ██╗██║ ╚████║╚██████╔╝   ██║   ███████║    ╚██████╗██║  ██║██║  ██║╚███╔███╔╝███████╗███████╗██║  ██║[/yellow]
[yellow]╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝    ╚═╝   ╚══════╝     ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚══╝╚══╝ ╚══════╝╚══════╝╚═╝  ╚═╝[/yellow]
                                                                                       [dim]Bitcoin network intelligence - sovereign edition[/dim]
"""
    console.print(banner)
