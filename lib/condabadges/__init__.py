from __future__ import annotations

from datetime import date, timedelta
import json
from math import floor, log10
import os
from urllib.parse import urlencode

import dask.dataframe as dd
import requests
import yaml


CACHE_DIR = "cache"
URLS_FILE = CACHE_DIR + "/urls.yaml"


def _check_cache_dir() -> None:
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)


def _create_badge_urls(
    project: str, colors: dict[str, str], downloads: dict[str, int],
) -> dict[str: str]:
    urls = {}
    for channel, color in colors.items():
        params = dict(
            color=color,
            label=channel,
            message=_format_downloads(downloads[channel]),
            style="flat",
        )

        arguments = f"?{urlencode(params)}"

        url = f"https://img.shields.io/static/v1{arguments}"
        urls[channel] = url

    print(urls)
    return urls


def _format_downloads(downloads: int) -> str:
    units = ["", "k", "M", "B"]  # Multiples of 1000.
    power = floor(log10(downloads))
    divisor = min(power // 3, len(units)-1)  # Index into units.
    unit = units[divisor]
    remainder = power - divisor*3
    downloads /= 10**(3*divisor)  # Convert to unit.
    if remainder == 0:
        downloads = f"{round(downloads, 1)}".rstrip(".0")
    else:
        downloads = f"{floor(downloads)}"

    return f"{downloads}{unit}/month"


def _prev_month_downloads(project: str) -> dict[str: int]:
    previous_month = date.today().replace(day=1) - timedelta(days=1)
    year = previous_month.year
    month = previous_month.month

    url = f"s3://anaconda-package-data/conda/monthly/{year}/{year}-{month:02d}.parquet"
    print(f"s3 url: {url}")

    ddf = dd.read_parquet(
        url,
        columns=("counts", "data_source", "pkg_name"),
        storage_options={"anon": True},
    )

    ddf = ddf[ddf.pkg_name==project]

    grouped = ddf.groupby("data_source").counts.sum().compute()
    nonzero = grouped[grouped > 0]

    monthly_downloads = nonzero.to_dict()
    print(monthly_downloads)

    return monthly_downloads


def download_badges() -> None:
    _check_cache_dir()

    with open(URLS_FILE, "r") as f:
        urls = yaml.safe_load(f)

    for channel, url in urls.items():
        local_filename = f"{CACHE_DIR}/contourpy_{channel}_monthly.svg"

        r = requests.get(url)
        r.raise_for_status()

        with open(local_filename, 'wb') as f:
            f.write(r.content)
        print(f"Written {local_filename}")


def run() -> None:
    project = "contourpy"
    colors = {
        "conda-forge": "a6d96a",
        "anaconda": "1a9641",
    }

    downloads = _prev_month_downloads(project)
    urls = _create_badge_urls(project, colors, downloads)

    # Store URLs in case of problems.
    _check_cache_dir()
    with open(URLS_FILE, "w") as f:
        yaml.dump(urls, f)

    download_badges()
