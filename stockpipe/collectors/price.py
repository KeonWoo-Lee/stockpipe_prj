from datetime import date, timedelta

from sqlalchemy import text

from stockpipe.collectors.base import BaseCollector


class PriceCollector(BaseCollector):
    """pykrx로 일봉 OHLCV 수집."""

    def collect(self, target_date: date | None = None) -> int:
        from pykrx.stock import get_market_ohlcv_by_date

        if target_date is None:
            target_date = date.today()

        start = target_date - timedelta(days=self.settings.LOOKBACK_DAYS)
        total = 0

        for code, name in self.settings.STOCK_CODES.items():
            df = get_market_ohlcv_by_date(
                start.strftime("%Y%m%d"),
                target_date.strftime("%Y%m%d"),
                code,
            )

            if df.empty:
                self.logger.warning("No data for %s (%s)", name, code)
                continue

            rows = self._to_rows(df, code, name)
            valid = self.validate(rows)
            self._upsert(valid)
            total += len(valid)
            self.logger.info("Collected %d rows for %s", len(valid), name)

        return total

    def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        for row in data:
            if row["close_price"] <= 0:
                self.logger.warning("Invalid close: %s", row)
                continue
            if row["high_price"] < row["low_price"]:
                self.logger.warning("High < Low: %s", row)
                continue
            if row["volume"] < 0:
                self.logger.warning("Negative volume: %s", row)
                continue
            valid.append(row)
        return valid

    def _to_rows(self, df, stock_code: str, stock_name: str) -> list[dict]:
        """DataFrame을 dict 리스트로 변환."""
        rows = []
        for trade_date, row in df.iterrows():
            rows.append({
                "stock_code": stock_code,
                "stock_name": stock_name,
                "trade_date": trade_date.date(),
                "open_price": float(row.iloc[0]),
                "high_price": float(row.iloc[1]),
                "low_price": float(row.iloc[2]),
                "close_price": float(row.iloc[3]),
                "volume": int(row.iloc[4]),
            })
        return rows

    def _upsert(self, rows: list[dict]) -> None:
        """DB에 upsert. 중복이면 업데이트."""
        if not rows:
            return
        stmt = text("""
            INSERT INTO raw_daily_prices
                (stock_code, stock_name, trade_date, open_price,
                 high_price, low_price, close_price, volume)
            VALUES
                (:stock_code, :stock_name, :trade_date, :open_price,
                 :high_price, :low_price, :close_price, :volume)
            ON CONFLICT (stock_code, trade_date)
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume
        """)
        self.db.execute(stmt, rows)
        self.db.commit()