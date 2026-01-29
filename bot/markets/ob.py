from typing import List, Tuple, Optional

class L2BookAnalyzer:
    """
    Analyzes L2 order book to:
    - Detect walls (large orders)
    - Calculate imbalance
    - Find optimal placement prices
    """
    def __init__(self, wall_threshold=3.0, imbalance_threshold=8.0):
        self.wall_threshold = wall_threshold
        self.imbalance_threshold = imbalance_threshold
        
        self.bids = []
        self.asks = []
        self.mid = None
        
    def update(self, msg):
        """Update the book"""
        levels = msg["levels"]
        self.bids = [(float(l["px"]), float(l["sz"])) for l in levels[0]]
        self.asks = [(float(l["px"]), float(l["sz"])) for l in levels[1]]
        if self.bids and self.asks:
            self.mid = (self.bids[0][0] + self.asks[0][0]) / 2
    
    def find_walls(self, levels: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Find walls (price, size)"""
        if not levels:
            return []
        
        sizes = [sz for _, sz in levels]
        avg = sum(sizes) / len(sizes)
        
        return [(px, sz) for px, sz in levels if sz > avg * self.wall_threshold]
    
    def get_imbalance(self, depth=10) -> Tuple[float, bool, str]:
        """
        Returns: (ratio, is_dangerous, side)
        ratio = bid_volume / ask_volume
        side = "BID_HEAVY" or "ASK_HEAVY" or "BALANCED"
        """
        bid_vol = sum(sz for _, sz in self.bids[:depth])
        ask_vol = sum(sz for _, sz in self.asks[:depth])
        
        ratio = bid_vol / ask_vol if ask_vol > 0 else 999
        
        is_dangerous = ratio > self.imbalance_threshold or ratio < (1/self.imbalance_threshold)
        
        if ratio > 1.5:
            side = "BID_HEAVY"
        elif ratio < 0.67:
            side = "ASK_HEAVY"
        else:
            side = "BALANCED"
        
        return ratio, is_dangerous, side
    
    def should_cancel(self, my_side: str) -> Tuple[bool, str]:
        """
        my_side: "BID" or "ASK"
        Returns: (should_cancel, reason)
        """
        ratio, is_dangerous, heavy_side = self.get_imbalance()
        
        if not is_dangerous:
            return False, ""
        
        # If you're on the heavy side, you'll get crushed
        if my_side == "BID" and heavy_side == "ASK_HEAVY" and ratio > 20:
            return True, f"Ratio {ratio:.1f}x asks>bids - dump imminent"
        
        if my_side == "ASK" and heavy_side == "BID_HEAVY" and ratio > 20:
            return True, f"Ratio {1/ratio:.1f}x bids>asks - pump imminent"
        
        return False, ""
    
    def get_optimal_prices(self, is_bid: bool, nb_levels: int) -> List[float]:
        """
        Place orders just in front of walls + in gaps
        Returns: List of optimal prices
        """
        levels = self.bids if is_bid else self.asks
        walls = self.find_walls(levels)
        prices = []
        
        # 1. Front-run walls
        tick = 0.01  # Adjust per instrument
        for wall_px, wall_sz in walls[:nb_levels]:
            price = wall_px + tick if is_bid else wall_px - tick
            prices.append(price)
        
        # 2. Fill with gaps
        if len(prices) < nb_levels:
            gaps = self._find_gaps(levels)
            for gap_px in gaps:
                if len(prices) >= nb_levels:
                    break
                prices.append(gap_px)
        
        # 3. If not enough, uniform spacing around mid
        if len(prices) < nb_levels:
            spread_range = 0.003  # 0.3%
            for i in range(nb_levels - len(prices)):
                offset = spread_range * (i + 1) / nb_levels
                price = self.mid * (1 - offset if is_bid else 1 + offset)
                prices.append(price)
        
        return sorted(prices, reverse=is_bid)[:nb_levels]
    
    def _find_gaps(self, levels: List[Tuple[float, float]]) -> List[float]:
        """Find price gaps in the book"""
        gaps = []
        for i in range(len(levels) - 1):
            px1, sz1 = levels[i]
            px2, sz2 = levels[i + 1]
            
            price_gap = abs(px2 - px1) / px1
            avg_sz = (sz1 + sz2) / 2
            
            # Gap = significant price difference AND low liquidity
            if price_gap > 0.0005 and avg_sz < 0.05:
                gaps.append((px1 + px2) / 2)
        
        return gaps[:5]