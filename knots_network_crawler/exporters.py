"""
Export functionality:
- JSON (full rich data)
- CSV (flat, analysis friendly)
- Graphviz DOT (for visualization in Gephi, Graphviz, etc. - great for sovereign network mapping)
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, List

from .models import Node


def _node_to_dict(n: Node) -> dict:
    return {
        "ip": n.ip,
        "port": n.port,
        "version": n.version,
        "subversion": n.subversion,
        "services": n.services,
        "services_listening": n.services_listening,
        "is_knots": n.is_knots,
        "user_agent": n.user_agent,
        "start_height": n.start_height,
        "latency_ms": n.latency_ms,
        "first_seen": n.first_seen,
        "last_seen": n.last_seen,
        "last_crawled": n.last_crawled,
        "crawl_count": n.crawl_count,
        "last_height_delta": n.last_height_delta,
        "country": n.geo.country,
        "country_code": n.geo.country_code,
        "city": n.geo.city,
        "asn": n.geo.asn,
        "asn_org": n.geo.asn_org,
        "latitude": n.geo.latitude,
        "longitude": n.geo.longitude,
    }


def export_json(nodes: Iterable[Node], path: Path) -> None:
    data = [_node_to_dict(n) for n in nodes]
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def export_csv(nodes: Iterable[Node], path: Path) -> None:
    nodes = list(nodes)
    if not nodes:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(_node_to_dict(nodes[0]).keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for n in nodes:
            w.writerow(_node_to_dict(n))


def export_graphviz(
    nodes: Iterable[Node],
    path: Path,
    *,
    only_knots: bool = False,
    only_listening: bool = False,
    max_nodes: int = 2500,
) -> None:
    """
    Produce a .dot file.

    Nodes are colored by type:
      - Yellow border + filled for Knots
      - Green for listening non-Knots
      - Gray otherwise

    This is extremely useful for visualizing clusters, AS concentration, etc.
    """
    filtered = []
    for n in nodes:
        if only_knots and not n.is_knots:
            continue
        if only_listening and not n.services_listening:
            continue
        filtered.append(n)
        if len(filtered) >= max_nodes:
            break

    lines: List[str] = []
    lines.append("digraph bitcoin_network {")
    lines.append('  graph [rankdir=LR, bgcolor="#111111", fontcolor="#ffcc00", fontname="DejaVu Sans"];')
    lines.append('  node [shape=circle, style="filled", fontcolor="#ffcc00", fontname="DejaVu Sans", fontsize=9];')
    lines.append('  edge [color="#444444", arrowsize=0.5];')

    for n in filtered:
        label_parts = [f"{n.ip}:{n.port}"]
        if n.is_knots:
            label_parts.append("KNOTS")
        if n.services_listening:
            label_parts.append("LISTEN")
        if n.start_height:
            label_parts.append(f"h={n.start_height}")
        if n.geo.country:
            label_parts.append(n.geo.country_code or n.geo.country[:2])

        label = "\\n".join(label_parts)
        color = "#ffcc00" if n.is_knots else ("#44aa44" if n.services_listening else "#666666")
        fillcolor = "#1a1a00" if n.is_knots else ("#001a00" if n.services_listening else "#0a0a0a")

        lines.append(
            f'  "{n.key()}" [label="{label}", color="{color}", fillcolor="{fillcolor}"];'
        )

    # We don't have real edges from the crawl (getaddr is not "I am connected to"),
    # so we emit isolated nodes. User can later correlate or use other tools.
    # This is still very valuable for ASN/country clustering analysis when imported in Gephi.

    lines.append("}")
    path.write_text("\n".join(lines), encoding="utf-8")
