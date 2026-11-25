from typing import Annotated, Literal

from pydantic import BaseModel, StringConstraints

Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]
Symbol = Annotated[str, StringConstraints(strip_whitespace=True, to_upper=True, pattern=r"^[A-Za-z][A-Za-z0-9.\-]{0,9}$")]
# Query-string symbol list, validated again after splitting.
SymbolList = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9.,\- ]{1,120}$")]

HistoryRange = Literal["1D", "1W", "1M", "3M", "1Y"]
AnalyticsRange = Literal["1M", "3M", "6M", "1Y"]


class WatchlistName(BaseModel):
    name: Name


class WatchlistSymbol(BaseModel):
    symbol: Symbol
