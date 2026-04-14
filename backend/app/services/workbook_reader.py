from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd


@dataclass(frozen=True)
class SheetGrid:
    sheet_name: str
    df: pd.DataFrame  # raw cell grid (object dtype)


class WorkbookReader:
    """
    MVP: read an .xlsx into a set of 2D grids (DataFrames) per sheet.
    We intentionally keep values "as seen" and do downstream normalization in extract/validate layers.
    """

    def read_xlsx(self, path: Path, *, max_rows: Optional[int] = None, max_cols: Optional[int] = None) -> list[SheetGrid]:
        # engine=openpyxl by default for xlsx
        xl = pd.ExcelFile(path)
        grids: list[SheetGrid] = []
        for sheet in xl.sheet_names:
            df = xl.parse(sheet_name=sheet, header=None, dtype=object)

            # Trim huge trailing empty regions to keep scanning fast.
            df = self._trim_empty(df)
            if max_rows is not None:
                df = df.iloc[:max_rows, :]
            if max_cols is not None:
                df = df.iloc[:, :max_cols]

            grids.append(SheetGrid(sheet_name=sheet, df=df))
        return grids

    def _trim_empty(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Find last non-empty row/col by scanning for any non-null / non-blank.
        non_blank = df.applymap(self._is_non_blank)
        if not non_blank.to_numpy().any():
            return df.iloc[:0, :0]

        row_any = non_blank.any(axis=1)
        col_any = non_blank.any(axis=0)
        last_row = int(row_any[row_any].index.max())
        last_col = int(col_any[col_any].index.max())
        return df.iloc[: last_row + 1, : last_col + 1]

    def _is_non_blank(self, v: Any) -> bool:
        if v is None:
            return False
        if isinstance(v, float) and pd.isna(v):
            return False
        if isinstance(v, str) and v.strip() == "":
            return False
        return True

