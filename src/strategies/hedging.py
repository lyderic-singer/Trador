"""Hedge engine: not published.

This module holds the cross-exchange hedge engine used by the production strategies: one hedge job per maker fill,
in-flight IOC accounting, gap repair, circuit breakers. It is part of the private strategy code and is therefore not
included in this public repository, together with the strategies themselves and their research tools.

What is public: the exchange engines (src/core), the adapters behind the common `Exchange` interface (src/adapters),
the strategy building blocks (market data, account tracking, executor, order tracking, batched gateway), logging and
Telegram (src/log), the offline test suite, and one complete example strategy, Enveloppe (src/strategies/enveloppe).
"""
