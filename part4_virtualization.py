#!/usr/bin/env python3
"""
AUCA E-commerce Project - Part 4: Static Visualizations

Reads the CSV outputs created by part3.py and generates four static PNG charts
plus a short interpretation report.

Supported input:
  1) The extracted Part 3 output directory, e.g. part3_output/
  2) The ZIP file containing that directory, e.g. part3_output.zip

Install dependencies:
    pip install pandas matplotlib

Examples (Windows):
    python part4_visualizations.py --input part3_output.zip
    python part4_visualizations.py --input part3_output
    python part4_visualizations.py --input part3_output.zip --output part4_output

If --input is omitted, the script looks for a folder named "part3_output"
beside this script.
"""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # Save static image files; no GUI window required.

import matplotlib.pyplot as plt
import pandas as pd


REQUIRED_FILES = {
    "customer_clv_engagement": "customer_clv_engagement",
    "customer_segment_summary": "customer_segment_summary",
    "country_performance": "country_performance",
    "engagement_vs_purchases": "engagement_vs_purchases",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate four static Part 4 charts from Part 3 Spark outputs."
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Part 3 output folder or ZIP file. Default: ./part3_output",
    )
    parser.add_argument(
        "--output",
        default="part4_output",
        help="Directory for PNG charts and interpretation notes (default: part4_output).",
    )
    parser.add_argument(
        "--top-countries",
        type=int,
        default=10,
        help="Number of countries to show in the country chart (default: 10).",
    )
    return parser.parse_args()


def find_csv_in_folder(folder: Path) -> Path:
    """Find a Spark part CSV in a folder, ignoring _SUCCESS and metadata files."""
    candidates = sorted(folder.glob("part-*.csv"))
    if not candidates:
        candidates = sorted(folder.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No CSV file found in: {folder}")
    return candidates[0]


def load_from_directory(root: Path):
    data = {}
    for key, relative_folder in REQUIRED_FILES.items():
        folder = root / relative_folder
        if not folder.exists():
            raise FileNotFoundError(f"Required Part 3 folder is missing: {folder}")
        csv_path = find_csv_in_folder(folder)
        data[key] = pd.read_csv(csv_path)

    summary_path = root / "part3_summary.json"
    summary = {}
    if summary_path.exists():
        with summary_path.open("r", encoding="utf-8") as handle:
            summary = json.load(handle)

    return data, summary


def find_zip_member(names, folder_name):
    prefix = folder_name.strip("/") + "/"
    matches = [
        name for name in names
        if name.startswith(prefix)
        and "/_temporary/" not in name
        and Path(name).name.startswith("part-")
        and name.lower().endswith(".csv")
    ]
    if not matches:
        # Support ZIPs where the top-level directory is not named part3_output.
        matches = [
            name for name in names
            if f"/{folder_name}/" in name
            and Path(name).name.startswith("part-")
            and name.lower().endswith(".csv")
        ]
    if not matches:
        raise FileNotFoundError(
            f"Could not find a Spark part CSV for '{folder_name}' inside the ZIP."
        )
    return sorted(matches)[0]


def load_from_zip(zip_path: Path):
    data = {}
    with zipfile.ZipFile(zip_path, "r") as archive:
        names = archive.namelist()

        for key, folder_name in REQUIRED_FILES.items():
            member = find_zip_member(names, folder_name)
            with archive.open(member) as file_obj:
                data[key] = pd.read_csv(file_obj)

        summary_candidates = [
            name for name in names
            if Path(name).name.lower() == "part3_summary.json"
        ]
        summary = {}
        if summary_candidates:
            summary = json.loads(archive.read(sorted(summary_candidates)[0]).decode("utf-8"))

    return data, summary


def require_columns(df: pd.DataFrame, columns, dataset_name):
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: {', '.join(missing)}. "
            f"Available columns: {', '.join(map(str, df.columns))}"
        )


def save_chart(path: Path):
    plt.tight_layout()
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close()


def chart_customer_revenue(clv: pd.DataFrame, output_dir: Path):
    require_columns(clv, ["lifetime_revenue"], "customer_clv_engagement")
    revenue = pd.to_numeric(clv["lifetime_revenue"], errors="coerce").dropna()
    revenue = revenue[revenue >= 0]
    if revenue.empty:
        raise ValueError("No valid values found in lifetime_revenue.")

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.hist(revenue, bins=35, edgecolor="black", linewidth=0.5)
    ax.set_title("Distribution of Customer Revenue")
    ax.set_xlabel("Observed-period customer revenue")
    ax.set_ylabel("Number of customers")
    ax.grid(axis="y", alpha=0.25)
    save_chart(output_dir / "01_customer_revenue_distribution.png")


def chart_customer_segments(segments: pd.DataFrame, output_dir: Path):
    require_columns(
        segments,
        ["customer_segment", "customers", "revenue"],
        "customer_segment_summary",
    )
    df = segments.copy()
    df["customers"] = pd.to_numeric(df["customers"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["customer_segment", "customers", "revenue"])
    df = df.sort_values("revenue", ascending=True)

    fig, ax1 = plt.subplots(figsize=(10, 5.8))
    y = range(len(df))
    bars = ax1.barh(y, df["customers"], label="Customers")
    ax1.set_yticks(list(y))
    ax1.set_yticklabels(df["customer_segment"])
    ax1.set_xlabel("Number of customers")
    ax1.set_title("Customer Segment Size and Revenue Contribution")
    ax1.grid(axis="x", alpha=0.25)

    ax2 = ax1.twiny()
    ax2.plot(df["revenue"], list(y), marker="o", linewidth=2, label="Revenue")
    ax2.set_xlabel("Observed-period revenue")

    for bar, value in zip(bars, df["customers"]):
        ax1.text(
            bar.get_width(), bar.get_y() + bar.get_height() / 2,
            f" {value:,.0f}", va="center", fontsize=8
        )

    save_chart(output_dir / "02_customer_segment_comparison.png")


def chart_country_revenue(countries: pd.DataFrame, output_dir: Path, top_n: int):
    require_columns(countries, ["country", "revenue"], "country_performance")
    df = countries.copy()
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["country", "revenue"])
    df = df.nlargest(max(1, top_n), "revenue").sort_values("revenue", ascending=True)
    if df.empty:
        raise ValueError("No valid country revenue rows found.")

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(df["country"].astype(str), df["revenue"])
    ax.set_title(f"Top {len(df)} Countries by Customer Revenue")
    ax.set_xlabel("Observed-period revenue")
    ax.set_ylabel("Country code")
    ax.grid(axis="x", alpha=0.25)
    save_chart(output_dir / "03_top_countries_by_revenue.png")


def chart_engagement(engagement: pd.DataFrame, output_dir: Path):
    require_columns(
        engagement,
        ["engagement_level", "average_customer_revenue", "average_page_views"],
        "engagement_vs_purchases",
    )
    df = engagement.copy()
    df["average_customer_revenue"] = pd.to_numeric(
        df["average_customer_revenue"], errors="coerce"
    )
    df["average_page_views"] = pd.to_numeric(
        df["average_page_views"], errors="coerce"
    )
    df = df.dropna(
        subset=["engagement_level", "average_customer_revenue", "average_page_views"]
    )

    preferred_order = {"Low": 0, "Medium": 1, "High": 2}
    df["_sort"] = df["engagement_level"].map(preferred_order).fillna(99)
    df = df.sort_values("_sort")
    if df.empty:
        raise ValueError("No valid engagement summary rows found.")

    fig, ax1 = plt.subplots(figsize=(9, 5.5))
    x = list(range(len(df)))
    bars = ax1.bar(x, df["average_customer_revenue"], label="Average revenue")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["engagement_level"])
    ax1.set_xlabel("Browsing engagement level")
    ax1.set_ylabel("Average observed-period customer revenue")
    ax1.set_title("Customer Revenue and Browsing Engagement")
    ax1.grid(axis="y", alpha=0.25)

    ax2 = ax1.twinx()
    ax2.plot(
        x, df["average_page_views"], marker="o", linewidth=2,
        label="Average page views"
    )
    ax2.set_ylabel("Average page views per customer")

    for bar, value in zip(bars, df["average_customer_revenue"]):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:,.0f}",
            ha="center", va="bottom", fontsize=8
        )

    save_chart(output_dir / "04_engagement_vs_average_revenue.png")


def build_interpretation(data, summary, output_dir: Path):
    segments = data["customer_segment_summary"].copy()
    countries = data["country_performance"].copy()
    engagement = data["engagement_vs_purchases"].copy()

    for col in ["customers", "completed_orders", "revenue"]:
        if col in segments:
            segments[col] = pd.to_numeric(segments[col], errors="coerce")
    countries["revenue"] = pd.to_numeric(countries["revenue"], errors="coerce")
    engagement["average_customer_revenue"] = pd.to_numeric(
        engagement["average_customer_revenue"], errors="coerce"
    )
    engagement["average_page_views"] = pd.to_numeric(
        engagement["average_page_views"], errors="coerce"
    )

    lines = [
        "PART 4: VISUALIZATION AND INSIGHTS",
        "=" * 42,
        "",
        "Charts generated from the Part 3 Spark output datasets.",
        "",
        "DATASET SNAPSHOT",
        f"Integrated customers: {summary.get('users_in_integrated_result', 'not provided')}",
        f"Transactions in snapshot: {summary.get('transactions_in_snapshot', 'not provided')}",
        f"Completed transactions: {summary.get('completed_transactions', 'not provided')}",
        f"Unique HBase sessions reported: {summary.get('unique_hbase_sessions', 'not provided')}",
        "",
        "1. CUSTOMER REVENUE DISTRIBUTION",
        "The histogram shows the distribution of observed-period customer revenue. "
        "Use it to describe spending variation and whether customer revenue is concentrated "
        "within a narrower or wider range.",
        "",
        "2. CUSTOMER SEGMENT COMPARISON",
    ]

    total_segment_revenue = segments["revenue"].sum()
    for _, row in segments.sort_values("revenue", ascending=False).iterrows():
        share = (
            row["revenue"] / total_segment_revenue * 100
            if pd.notna(total_segment_revenue) and total_segment_revenue else 0
        )
        lines.append(
            f"- {row['customer_segment']}: {row['customers']:,.0f} customers; "
            f"{row['completed_orders']:,.0f} completed orders; "
            f"revenue {row['revenue']:,.2f} ({share:.1f}% of segment revenue)."
        )

    lines.extend(["", "3. COUNTRY PERFORMANCE"])
    top_country = countries.dropna(subset=["revenue"]).nlargest(1, "revenue")
    if not top_country.empty:
        row = top_country.iloc[0]
        lines.append(
            f"The highest-revenue country is {row['country']}, with revenue "
            f"{row['revenue']:,.2f}."
        )
    lines.append(
        "The chart displays the highest-revenue countries in the available country summary."
    )

    lines.extend(["", "4. ENGAGEMENT AND PURCHASING"])
    for _, row in engagement.iterrows():
        lines.append(
            f"- {row['engagement_level']} engagement: average customer revenue "
            f"{row['average_customer_revenue']:,.2f}; average page views "
            f"{row['average_page_views']:,.1f}."
        )

    correlation = summary.get("revenue_session_pageview_pearson_correlation")
    lines.extend([
        "",
        "INTERPRETATION AND LIMITATIONS",
        f"Revenue/page-view Pearson correlation: {correlation if correlation is not None else 'not provided'}.",
        "- Correlation describes association; it does not establish that browsing causes purchases.",
        "- The field named lifetime_revenue is treated here as observed-period revenue within "
        "the available dataset, not as a predictive customer lifetime value model.",
        "- The MongoDB transaction data represents the snapshot recorded in the Part 3 summary.",
        "- Revenue currency is not labelled because the supplied output does not specify a currency.",
        "",
        "FILES",
        "- 01_customer_revenue_distribution.png",
        "- 02_customer_segment_comparison.png",
        "- 03_top_countries_by_revenue.png",
        "- 04_engagement_vs_average_revenue.png",
        "- part4_interpretation.txt",
    ])

    output_file = output_dir / "part4_interpretation.txt"
    output_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    script_dir = Path(__file__).resolve().parent

    input_path = Path(args.input).expanduser() if args.input else script_dir / "part3_output"
    if not input_path.is_absolute():
        input_path = (Path.cwd() / input_path).resolve()

    output_dir = Path(args.output).expanduser()
    if not output_dir.is_absolute():
        output_dir = (Path.cwd() / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        if input_path.is_file() and input_path.suffix.lower() == ".zip":
            print(f"[INFO] Reading Part 3 results from ZIP: {input_path}")
            data, summary = load_from_zip(input_path)
        elif input_path.is_dir():
            print(f"[INFO] Reading Part 3 results from folder: {input_path}")
            data, summary = load_from_directory(input_path)
        else:
            raise FileNotFoundError(
                f"Input not found: {input_path}\n"
                "Pass the Part 3 output folder or part3_output.zip using --input."
            )

        print("[INFO] Generating static charts...")
        chart_customer_revenue(data["customer_clv_engagement"], output_dir)
        chart_customer_segments(data["customer_segment_summary"], output_dir)
        chart_country_revenue(data["country_performance"], output_dir, args.top_countries)
        chart_engagement(data["engagement_vs_purchases"], output_dir)
        build_interpretation(data, summary, output_dir)

        print("\n[SUCCESS] PART 4 VISUALIZATIONS COMPLETED")
        print(f"[SUCCESS] Output directory: {output_dir}")
        for file_path in sorted(output_dir.glob("*")):
            if file_path.is_file():
                print(f"  - {file_path.name}")

    except Exception as exc:
        print(f"\n[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
