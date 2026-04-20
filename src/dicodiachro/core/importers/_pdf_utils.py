from __future__ import annotations


def equal_width_breakpoints(width: float, columns: int) -> list[float]:
    return [width * i / columns for i in range(1, columns)]


def column_breakpoints(words: list[dict], *, width: float, columns: int) -> list[float]:
    if columns <= 1:
        return []
    x_positions = sorted(
        {float(w.get("x0", 0.0)) for w in words if str(w.get("text", "")).strip()}
    )
    if len(x_positions) < columns:
        return equal_width_breakpoints(width, columns)
    gaps = [(x_positions[i + 1] - x_positions[i], i) for i in range(len(x_positions) - 1)]
    positive = [g for g in gaps if g[0] > 0.0]
    if len(positive) < columns - 1:
        return equal_width_breakpoints(width, columns)
    selected = sorted(positive, key=lambda t: t[0], reverse=True)[: columns - 1]
    indices = sorted(i for _, i in selected)
    bp = [(x_positions[i] + x_positions[i + 1]) / 2 for i in indices]
    return bp if len(bp) == columns - 1 else equal_width_breakpoints(width, columns)


def column_index(x0: float, breakpoints: list[float]) -> int:
    for i, threshold in enumerate(breakpoints):
        if x0 < threshold:
            return i
    return len(breakpoints)


def split_words_by_column(words: list[dict], breakpoints: list[float]) -> list[list[dict]]:
    cols: list[list[dict]] = [[] for _ in range(len(breakpoints) + 1)]
    for w in words:
        cols[column_index(float(w.get("x0", 0.0)), breakpoints)].append(w)
    return cols
