# investment-qlib — Qlib 学习项目骨架

这是一个用于学习和快速上手 `qlib`（量化研究库）的初始项目结构。包含依赖清单、示例脚本、以及最小化的 `qlib` 初始化工具。

快速开始

1. 创建并激活虚拟环境（示例使用 `venv`）：

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .\.venv\Scripts\activate
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

3. 按需准备数据或使用 qlib 提供的样例数据，编辑 `examples/quickstart.py` 中的 `provider_uri`。

4. 运行示例：

```bash
python examples/quickstart.py
```

文件结构

- `requirements.txt` - 依赖列表
- `src/qlib_project/` - 工具函数与项目代码入口
- `examples/quickstart.py` - 最小示例：初始化 qlib 并打印状态
- `notebooks/01_quickstart.md` - 快速上手说明与代码片段

后续建议

- 若需完整回测/训练示例，可请求我添加基于 `qlib.contrib` 的策略示例与 notebook。
