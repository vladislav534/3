# calculator.py
from typing import Dict, List, Optional
from config import PERCENT_DECIMALS

def build_pairwise_table(labels: List[str], prices: Dict[str, Optional[float]]):
    """
    Попарная таблица со симметричной процентной разницей по формуле:
      pct = 100 * (pi - pj) / ((pi + pj) / 2)

    Возвращает: {"labels": labels, "matrix": matrix, "mean": None}
    Если pi или pj отсутствуют, ячейка = "-"
    Если (pi + pj) == 0, ячейка = "-"
    """
    matrix = []
    for i in labels:
        row = []
        pi = prices.get(i)
        for j in labels:
            pj = prices.get(j)
            if pi is None or pj is None:
                cell = "-"
            else:
                denom = (pi + pj) / 2.0
                # MIN_DENOM guards against division by near-zero -> false large percents
                MIN_DENOM = 1e-9
                if abs(denom) <= MIN_DENOM:
                    cell = "-"
                else:
                    pct = 100.0 * (pi - pj) / denom
                    cell = f"{pct:.{PERCENT_DECIMALS}f}%"
            row.append(cell)
        matrix.append(row)
    return {"labels": labels, "matrix": matrix, "mean": None}
