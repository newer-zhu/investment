# 股票推荐系统 - 前端

这是一个用于展示每日股票推荐结果的React前端应用。

## 功能特性

- 📊 展示每日推荐的股票数据
- 🔍 支持按行业筛选
- 📈 支持按总分筛选
- 🔄 支持多列排序（点击表头）
- 📱 响应式设计，支持移动端

## 技术栈

- React 18
- Vite
- PapaParse (CSV解析)

## 安装和运行

### 1. 安装依赖

```bash
cd frontend
npm install
```

### 2. 配置数据源

有两种方式提供CSV数据：

#### 方式一：静态文件（开发阶段，默认）

将CSV文件复制到 `public/output/` 目录：

```bash
# 在项目根目录执行
mkdir -p frontend/public/output
cp output/picked_stocks_*.csv frontend/public/output/
```

#### 方式二：后端API（推荐，便于扩展）

1. 启动后端API服务（使用项目根目录的 `backend_api_example.py`）：

```bash
# 安装依赖
pip install flask flask-cors pandas

# 启动后端
python backend_api_example.py
```

2. 创建 `frontend/.env` 文件启用API模式：

```env
VITE_USE_API=true
VITE_API_BASE_URL=http://localhost:8000
```

前端会自动切换到API模式，从后端获取数据。

### 3. 启动开发服务器

```bash
npm run dev
```

应用将在 http://localhost:3000 启动

### 4. 构建生产版本

```bash
npm run build
```

构建产物在 `dist` 目录

## 项目结构

```
frontend/
├── public/           # 静态资源
│   └── output/      # CSV文件目录（开发用）
├── src/
│   ├── components/   # React组件
│   │   ├── StockTable.jsx
│   │   └── StockTable.css
│   ├── App.jsx       # 主应用组件
│   ├── App.css       # 主应用样式
│   ├── main.jsx      # 入口文件
│   └── index.css     # 全局样式
├── index.html        # HTML模板
├── vite.config.js    # Vite配置
└── package.json      # 项目配置
```

## 后端API

项目已包含一个Flask后端API示例（`backend_api_example.py`），提供以下接口：

- `GET /api/dates` - 获取所有可用日期列表
- `GET /api/stocks/<date>` - 获取指定日期的股票数据（JSON格式）
- `GET /api/stocks/<date>/csv` - 下载指定日期的CSV文件
- `GET /api/health` - 健康检查

详细使用说明请参考项目根目录的 `backend_api_example.py` 文件。

## 后续扩展建议

1. ✅ **后端集成**：已提供Flask后端API示例
2. **数据可视化**：集成Chart.js或ECharts，展示股票趋势图
3. **实时更新**：使用WebSocket实现实时数据推送
4. **用户系统**：添加登录、收藏等功能
5. **更多筛选**：添加价格区间、市值区间等筛选条件
6. **导出功能**：支持导出为Excel或PDF
7. **数据缓存**：使用localStorage或IndexedDB缓存数据
8. **搜索功能**：支持按股票代码或名称搜索

## 注意事项

- 确保CSV文件格式与代码中的字段名匹配
- 如果使用后端API，需要配置CORS
- 生产环境建议使用后端API而不是静态文件

