---
description: Project instruction for the qlib-based quant system
# applyTo: '/mnt/f/Code/investment/investment-qlib/**/*'
---

<!-- Tip: Use /create-instructions in chat to generate content with agent assistance -->

This project builds a short-term, low-frequency quant system on Qlib 0.9.7.

Follow these rules when working in this repository:

- Treat Qlib 0.9.7 as the project baseline unless the user explicitly asks to migrate.
- Prefer short-horizon, low-turnover strategy logic over intraday or high-frequency assumptions.
- Keep changes compatible with the existing Qlib project structure under investment-qlib.
- Use Qlib-native APIs, data handlers, and backtesting patterns when they fit the task.
- Avoid introducing framework changes that would broaden the scope beyond the current quant workflow.
- When modifying strategy, data, or backtest code, preserve the project’s current data flow and execution model unless the user requests a redesign.
- If a requirement is ambiguous, ask whether it should optimize for signal quality, turnover, risk control, or execution simplicity before making broad changes.

When reviewing or generating code, prioritize:

- robustness of research/backtest results,
- clarity of data lineage,
- low operational complexity,
- and consistency with the existing qlib-based implementation.