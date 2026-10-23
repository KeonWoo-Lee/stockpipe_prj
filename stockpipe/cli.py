# src/stockpipe/cli.py
import click

from stockpipe.config import Settings
from stockpipe.db import get_engine, get_session_factory, init_db


@click.group()
def main():
    """StockPipe: 한국 주식 투자 모니터링 파이프라인."""


@main.command()
def setup():
    """DB 테이블 초기화."""
    settings = Settings()
    engine = get_engine(settings)
    init_db(engine)
    click.echo("DB initialized.")


@main.command()
@click.option("--prices", is_flag=True, help="주가 수집")
def collect(prices):
    """데이터 수집."""
    settings = Settings()
    engine = get_engine(settings)
    session_factory = get_session_factory(engine)

    with session_factory() as session:
        if prices:
            from stockpipe.collectors.price import PriceCollector

            collector = PriceCollector(settings, session)
            count = collector.collect()
            click.echo(f"Collected {count} price rows.")

    if not prices:
        click.echo("No collector selected. Use --prices")