"""Indian stock-option trading costs, per share — shared by the ADR stress test and the paper trader.

Current (post-Oct-2024) rates; bid/ask slippage is handled by the caller.
"""
STT_SELL, TXN, SEBI, STAMP_BUY, GST = 0.001, 0.0003503, 10 / 1e7, 0.00003, 0.18
FUT_STT_SELL, FUT_TXN = 0.0002, 0.0000173
BROKERAGE, DEFAULT_LOT, TICK = 20.0, 225, 0.05


def leg_fees(price: float, qty_sign: int, lot: float) -> float:
    """Statutory fees + brokerage for one leg trade, per share. qty_sign +1 buy / -1 sell."""
    fees = price * TXN + price * SEBI + BROKERAGE / lot
    return fees * (1 + GST) + price * (STT_SELL if qty_sign < 0 else STAMP_BUY)
