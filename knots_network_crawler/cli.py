"""
CLI entrypoint using Typer + Rich.

Commands:
  crawl     - Run the crawler (normal or --aggressive)
  stats     - Global + geo statistics
  view      - Interactive-ish filtered table views
  export    - Dump data in useful formats
  info      - Show config / GeoIP status / recent sessions

All commands support --db to point at a specific database.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Confirm

from .config import CrawlerConfig, load_config
from .crawler import CrawlStats, KnotsNetworkCrawler
from .database import Database
from .exporters import export_csv, export_graphviz, export_json
from .views import (
    console,
    print_banner,
    render_geo_breakdown,
    render_global_stats,
    render_knots_view,
    render_listening_view,
    render_node_table,
    render_top_height_view,
)

app = typer.Typer(
    name="knots-network-crawler",
    help="Serious Bitcoin network crawler focused on Knots + reachable nodes. Sovereign intelligence tool.",
    add_completion=False,
    rich_markup_mode="rich",
)


def _get_db(db_path: Optional[Path]) -> Database:
    path = db_path or Path("data/nodes.db")
    return Database(path)


@app.callback()
def main_callback() -> None:
    print_banner()


@app.command("crawl")
def cmd_crawl(
    mode: str = typer.Option("normal", "--mode", "-m", help="normal | aggressive"),
    concurrency: Optional[int] = typer.Option(None, "--concurrency", "-c", help="Max concurrent connections"),
    max_nodes: Optional[int] = typer.Option(None, "--max-nodes", help="Stop after attempting this many nodes"),
    duration: Optional[int] = typer.Option(None, "--duration", help="Max duration in seconds"),
    db: Optional[Path] = typer.Option(None, "--db", help="Path to SQLite database"),
    geoip_city: Optional[Path] = typer.Option(None, "--geo-city", help="Path to GeoLite2-City.mmdb"),
    geoip_asn: Optional[Path] = typer.Option(None, "--geo-asn", help="Path to GeoLite2-ASN.mmdb"),
    only_known: bool = typer.Option(False, "--only-known", help="Do not discover new nodes, only refresh existing"),
    seeds: Optional[List[str]] = typer.Option(None, "--seed", help="Additional bootstrap seeds (ip:port or hostname)"),
    no_fork_detect: bool = typer.Option(False, "--no-fork-detect", help="Skip per-peer BLAKE2b chain check (fast discovery phase; verify separately)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
) -> None:
    """Run a crawl against the Bitcoin mainnet P2P network."""

    cfg: CrawlerConfig = load_config(
        db_path=str(db) if db else None,
        mode=mode,
        max_concurrent=concurrency,
        max_nodes=max_nodes,
        duration=duration,
        geoip_city=str(geoip_city) if geoip_city else None,
        geoip_asn=str(geoip_asn) if geoip_asn else None,
        only_update_known=only_known,
        verbose=False,
        extra_seeds=seeds,
    )
    if no_fork_detect:
        cfg.fork_detect = False

    console.print(Panel.fit(
        f"[yellow]Mode:[/yellow] {cfg.mode.upper()}   "
        f"[yellow]Concurrency:[/yellow] {cfg.max_concurrent}   "
        f"[yellow]Max nodes:[/yellow] {cfg.max_nodes_to_crawl}   "
        f"[yellow]GeoIP:[/yellow] {'present' if cfg.geoip_city_mmdb or True else 'checking...'}",
        title="Crawl Configuration",
        border_style="yellow",
    ))

    if not yes and not Confirm.ask("Start crawling now?", default=True):
        raise typer.Exit()

    db = _get_db(cfg.db_path)
    stats_container: dict = {"stats": CrawlStats()}

    def progress_cb(event: str, data: dict) -> None:
        # We update a live display in the main loop
        stats_container["stats"].discovered = max(stats_container["stats"].discovered, data.get("discovered", stats_container["stats"].discovered))
        # The actual live numbers come from the crawler object; we refresh on events
        pass

    crawler = KnotsNetworkCrawler(cfg, db, progress=progress_cb)

    async def _run_with_live():
        live_stats = crawler.stats
        # We patch a reference so progress can see queue size etc. (simple)
        with Live(console=console, refresh_per_second=1.5, transient=False) as live:
            def live_progress(event: str, data: dict):
                # Mutate in place for Live
                if event == "peer_ok":
                    live_stats.succeeded = crawler.stats.succeeded
                    live_stats.discovered = crawler.stats.discovered
                    live_stats.knots_found = crawler.stats.knots_found
                    live_stats.listening_found = crawler.stats.listening_found
                    live_stats.max_height = crawler.stats.max_height
                    # attach dynamic attrs for display
                    setattr(live_stats, "qsize", data.get("queue", 0))

                line = (
                    f"[yellow bold]RUNNING[/yellow bold]  "
                    f"crawled [bold]{live_stats.succeeded}[/bold]  "
                    f"disc [bold]{live_stats.discovered}[/bold]  "
                    f"knots [bold green]{live_stats.knots_found}[/bold green]  "
                    f"listen [bold]{live_stats.listening_found}[/bold]  "
                    f"h=[bold]{live_stats.max_height:,}[/bold]  "
                    f"q={getattr(live_stats, 'qsize', '?')}  "
                    f"t={live_stats.elapsed:.0f}s"
                )
                live.update(Panel(line, border_style="yellow", title="knots-network-crawler live"))

            crawler.progress = live_progress
            final_stats = await crawler.run()
            live.stop()
            console.print("\n[yellow]Crawl completed.[/yellow]")
            # Show quick summary
            render_global_stats(await db.get_stats())

    asyncio.run(_run_with_live())


@app.command("stats")
def cmd_stats(
    db: Optional[Path] = typer.Option(None, "--db"),
    geo: bool = typer.Option(False, "--geo", help="Show full country + ASN breakdown"),
) -> None:
    """Show rich statistics from the database."""
    d = _get_db(db)
    stats = asyncio.run(_get_stats(d))
    geo_stats = asyncio.run(_get_geo_stats(d)) if geo else None
    render_global_stats(stats, geo_stats)
    if geo and geo_stats:
        render_geo_breakdown(geo_stats)


async def _get_stats(db: Database):
    await db.connect()
    return await db.get_stats()


async def _get_geo_stats(db: Database):
    await db.connect()
    return await db.get_geo_stats()


@app.command("view")
def cmd_view(
    db: Optional[Path] = typer.Option(None, "--db"),
    knots: bool = typer.Option(False, "--knots", help="Show only Bitcoin Knots nodes"),
    listening: bool = typer.Option(False, "--listening", "-l", help="Show only reachable (NODE_NETWORK) nodes"),
    top: bool = typer.Option(False, "--top", help="Show nodes with highest reported height"),
    limit: int = typer.Option(50, "--limit", "-n"),
) -> None:
    """Browse interesting subsets of the node database."""
    d = _get_db(db)

    async def _do_view():
        await d.connect()
        if knots:
            nodes = await d.get_knots_nodes(limit=limit)
            render_node_table(nodes, title=f"Bitcoin Knots nodes (showing {len(nodes)})", max_rows=limit)
        elif listening:
            nodes = await d.get_listening_nodes(limit=limit)
            render_node_table(nodes, title=f"Reachable listening nodes (showing {len(nodes)})", max_rows=limit)
        elif top:
            nodes = await d.get_top_by_height(limit=limit)
            render_node_table(nodes, title=f"Highest block height reporters (showing {len(nodes)})", max_rows=limit)
        else:
            # Default: recent interesting mix
            nodes = await d.get_all_nodes(limit=limit)
            render_node_table(nodes, title=f"Most recently seen nodes (showing {len(nodes)})", max_rows=limit)

    asyncio.run(_do_view())


@app.command("export")
def cmd_export(
    db: Optional[Path] = typer.Option(None, "--db"),
    fmt: str = typer.Option("json", "--format", "-f", help="json | csv | dot"),
    output: Path = typer.Option(..., "--output", "-o", help="Output file path"),
    only_knots: bool = typer.Option(False, "--only-knots"),
    only_listening: bool = typer.Option(False, "--only-listening"),
    limit: Optional[int] = typer.Option(None, "--limit"),
) -> None:
    """Export the node dataset for external analysis or graphing."""
    d = _get_db(db)

    async def _do_export():
        await d.connect()
        nodes = await d.get_all_nodes(limit=limit)
        if only_knots:
            nodes = [n for n in nodes if n.is_knots]
        if only_listening:
            nodes = [n for n in nodes if n.services_listening]

        output.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "json":
            export_json(nodes, output)
        elif fmt == "csv":
            export_csv(nodes, output)
        elif fmt in ("dot", "graphviz"):
            export_graphviz(nodes, output, only_knots=only_knots, only_listening=only_listening)
        else:
            console.print(f"[red]Unknown format: {fmt}[/red]")
            raise typer.Exit(1)

        console.print(f"[yellow]Exported {len(nodes)} nodes to[/yellow] {output}")

    asyncio.run(_do_export())


@app.command("verify")
def cmd_verify(
    db: Optional[Path] = typer.Option(None, "--db"),
    concurrency: int = typer.Option(12, "--concurrency", "-c", help="Parallel membership probes"),
    limit: int = typer.Option(20000, "--limit", help="Max candidates to verify"),
) -> None:
    """Phase 2: verify BLAKE2b membership + chain-verified height for reachable
    Knots candidates (version + getheaders probes only, no getaddr).

    The height reference (our tip, sampled once) is passed via env so every node
    is measured against the same point: CENSUS_TIP_HEIGHT, CENSUS_TIP_HASH,
    CENSUS_TIP_HEADER (raw 80-byte header hex), CENSUS_LOCATORS (JSON [[hash,height],…])."""
    import os, json
    cfg = load_config(db_path=str(db) if db else None)
    d = _get_db(cfg.db_path)
    from .verify import verify_candidates

    tip_height = int(os.environ["CENSUS_TIP_HEIGHT"]) if os.environ.get("CENSUS_TIP_HEIGHT") else None
    tip_hash = os.environ.get("CENSUS_TIP_HASH") or None
    tip_header = os.environ.get("CENSUS_TIP_HEADER") or None
    locators = json.loads(os.environ["CENSUS_LOCATORS"]) if os.environ.get("CENSUS_LOCATORS") else None

    async def _run():
        await d.connect()
        candidates = await d.get_fork_candidates(limit)
        console.print(f"[yellow]Verifying[/yellow] {len(candidates)} candidates at concurrency {concurrency}…")
        counts = await verify_candidates(d, cfg, candidates, concurrency=concurrency,
                                         tip_height=tip_height, tip_hash=tip_hash,
                                         tip_header=tip_header, locators=locators)
        console.print(f"[green]Done:[/green] {counts['fork']} BLAKE2b / {counts['checked']} checked "
                      f"({counts.get('heights',0)} heights)")

    asyncio.run(_run())


@app.command("info")
def cmd_info(db: Optional[Path] = typer.Option(None, "--db")) -> None:
    """Show environment, GeoIP status, recent crawl sessions."""
    cfg = load_config()
    d = _get_db(db or cfg.db_path)

    console.print(Panel.fit(
        f"[yellow]Database:[/yellow] {d.db_path}\n"
        f"[yellow]Mode default:[/yellow] {cfg.mode}\n"
        f"[yellow]Concurrency default:[/yellow] {cfg.max_concurrent}\n"
        f"[yellow]GeoIP status:[/yellow] {cfg.geoip_city_mmdb or 'auto-detect'}",
        title="Configuration",
        border_style="yellow",
    ))

    # GeoIP live check
    from .geoip import get_geoip_resolver
    geo = get_geoip_resolver(cfg.geoip_city_mmdb, cfg.geoip_asn_mmdb)
    console.print(Panel(geo.status(), title="GeoIP Resolver", border_style="yellow"))

    async def _sessions():
        await d.connect()
        sessions = await d.get_recent_sessions(8)
        if sessions:
            t = Table(title="Recent Crawl Sessions", border_style="yellow", title_style="yellow")
            t.add_column("Started", style="yellow")
            t.add_column("Mode")
            t.add_column("Crawled")
            t.add_column("Knots")
            t.add_column("Max H")
            for s in sessions:
                t.add_row(
                    str(s.get("started_at", ""))[:19],
                    s.get("mode", ""),
                    str(s.get("nodes_crawled", "")),
                    str(s.get("knots_found", "")),
                    str(s.get("max_height_seen", "")),
                )
            console.print(t)

    asyncio.run(_sessions())


if __name__ == "__main__":
    app()
