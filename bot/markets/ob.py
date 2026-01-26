from typing import List, Tuple, Optional

class L2BookAnalyzer:
    def __init__(self, wall_threshold=3.0, imbalance_threshold=8.0):
        self.wall_threshold = wall_threshold
        self.imbalance_threshold = imbalance_threshold
        
        self.bids = []
        self.asks = []
        self.mid = None
        
    def update(self, msg):
        """Met à jour le book"""
        levels = msg["levels"]
        self.bids = [(float(l["px"]), float(l["sz"])) for l in levels[0]]
        self.asks = [(float(l["px"]), float(l["sz"])) for l in levels[1]]
        if self.bids and self.asks:
            self.mid = (self.bids[0][0] + self.asks[0][0]) / 2
    
    def find_walls(self, levels: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Trouve les walls (prix, taille)"""
        if not levels:
            return []
        
        sizes = [sz for _, sz in levels]
        avg = sum(sizes) / len(sizes)
        
        return [(px, sz) for px, sz in levels if sz > avg * self.wall_threshold]
    
    def get_imbalance(self, depth=10) -> Tuple[float, bool, str]:
        """
        Returns: (ratio, is_dangerous, side)
        ratio = bid_volume / ask_volume
        side = "BID_HEAVY" ou "ASK_HEAVY" ou "BALANCED"
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
        my_side: "BID" ou "ASK"
        Returns: (should_cancel, reason)
        """
        ratio, is_dangerous, heavy_side = self.get_imbalance()
        
        if not is_dangerous:
            return False, ""
        
        # Si t'es du côté lourd, tu vas te faire écraser
        if my_side == "BID" and heavy_side == "BID_HEAVY":
            return True, f"Ratio {ratio:.1f}x bids>asks - dump imminent"
        
        if my_side == "ASK" and heavy_side == "ASK_HEAVY":
            return True, f"Ratio {1/ratio:.1f}x asks>bids - pump imminent"
        
        return False, ""
    
    def get_optimal_prices(self, is_bid: bool, nb_levels: int) -> List[float]:
        """
        Place les ordres juste devant les walls + dans les gaps
        Returns: Liste de prix optimaux
        """
        levels = self.bids if is_bid else self.asks
        walls = self.find_walls(levels)
        prices = []
        
        # 1. Se placer devant les walls (front-run)
        tick = 0.01  # Ajuste selon ton instrument
        for wall_px, wall_sz in walls[:nb_levels]:
            price = wall_px + tick if is_bid else wall_px - tick
            prices.append(price)
        
        # 2. Combler avec des gaps
        if len(prices) < nb_levels:
            gaps = self._find_gaps(levels)
            for gap_px in gaps:
                if len(prices) >= nb_levels:
                    break
                prices.append(gap_px)
        
        # 3. Si pas assez, spacing uniforme autour du mid
        if len(prices) < nb_levels:
            spread_range = 0.003  # 0.3%
            for i in range(nb_levels - len(prices)):
                offset = spread_range * (i + 1) / nb_levels
                price = self.mid * (1 - offset if is_bid else 1 + offset)
                prices.append(price)
        
        return sorted(prices, reverse=is_bid)[:nb_levels]
    
    def _find_gaps(self, levels: List[Tuple[float, float]]) -> List[float]:
        """Trouve les gaps de prix dans le book"""
        gaps = []
        for i in range(len(levels) - 1):
            px1, sz1 = levels[i]
            px2, sz2 = levels[i + 1]
            
            price_gap = abs(px2 - px1) / px1
            avg_sz = (sz1 + sz2) / 2
            
            # Gap = écart de prix significatif ET peu de liquidité
            if price_gap > 0.0005 and avg_sz < 0.05:
                gaps.append((px1 + px2) / 2)
        
        return gaps[:5]


# Intégration dans EnveloppePerpHL
async def on_l2Book(self, msg):
    """Version compacte et puissante"""
    if not hasattr(self, 'l2'):
        self.l2 = L2BookAnalyzer(wall_threshold=3.0, imbalance_threshold=8.0)
    
    self.l2.update(msg)
    
    # Check si on doit cancel
    bid_orders = [o for o in self.orders if o["side"] == "B"]
    ask_orders = [o for o in self.orders if o["side"] == "A"]
    
    if bid_orders:
        should_cancel, reason = self.l2.should_cancel("BID")
        if should_cancel:
            await self._log.warning(f"⚠️ Cancel bids: {reason}")
            cancels = [{"a": self.instrument.asset, "o": o["oid"]} for o in bid_orders]
            await self.hl.exchange.bulk_cancel(True, cancels)
    
    if ask_orders:
        should_cancel, reason = self.l2.should_cancel("ASK")
        if should_cancel:
            await self._log.warning(f"⚠️ Cancel asks: {reason}")
            cancels = [{"a": self.instrument.asset, "o": o["oid"]} for o in ask_orders]
            await self.hl.exchange.bulk_cancel(True, cancels)


# Version améliorée de new_orders qui utilise L2
def new_orders_with_l2(self, sma, mid, usd_balance, coin_balance):
    """Place les ordres aux meilleurs endroits selon le L2"""
    
    def _orders(balance, is_bid):
        if balance <= 0:
            return []
        
        # Calcul du nombre d'ordres possibles
        value_per_order = balance / self.levels
        nb_orders = self.levels if value_per_order > self.min_order else int(balance // self.min_order)
        
        if nb_orders == 0:
            return []
        
        # Taille par ordre
        size = (value_per_order / mid) if value_per_order > self.min_order else (self.min_order / mid)
        
        # Prix optimaux selon L2 book (au lieu du spread fixe)
        if hasattr(self, 'l2') and self.l2.mid:
            optimal_prices = self.l2.get_optimal_prices(is_bid, nb_orders)
        else:
            # Fallback sur spread classique
            optimal_prices = []
            for i in range(nb_orders):
                spread = ((self.spread_max - self.spread_min) * (i / max(1, self.levels - 1))) + self.spread_min
                price = sma * (1 - spread if is_bid else 1 + spread)
                optimal_prices.append(price)
        
        # Création des ordres
        orders = []
        for price in optimal_prices:
            order = get_order(self.instrument, is_bid, size, price, {"limit": {"tif": "Alo"}}, False, None)
            orders.append(order)
        
        return orders
    
    coin_balance_value = mid * coin_balance
    new_asks = _orders(coin_balance_value, False)
    new_bids = _orders(usd_balance, True)
    
    return new_asks[::-1], new_bids[::-1]