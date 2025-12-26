#!/bin/bash

echo "===================================="
echo "股票推荐系统 - 前端项目设置"
echo "===================================="
echo ""

echo "[1/3] 检查Node.js..."
if ! command -v node &> /dev/null; then
    echo "错误: 未找到Node.js，请先安装Node.js"
    echo "下载地址: https://nodejs.org/"
    exit 1
fi
echo "Node.js已安装"
echo ""

echo "[2/3] 安装依赖包..."
npm install
if [ $? -ne 0 ]; then
    echo "错误: 依赖安装失败"
    exit 1
fi
echo "依赖安装完成"
echo ""

echo "[3/3] 复制CSV文件到public目录..."
if [ ! -d "../output" ]; then
    echo "警告: 未找到output目录，请手动复制CSV文件到 public/output/ 目录"
else
    mkdir -p public/output
    cp ../output/picked_stocks_*.csv public/output/ 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "CSV文件已复制"
    else
        echo "警告: CSV文件复制失败，请手动复制"
    fi
fi
echo ""

echo "===================================="
echo "设置完成！"
echo "===================================="
echo ""
echo "启动开发服务器: npm run dev"
echo "构建生产版本: npm run build"
echo ""
echo "提示: 如需使用后端API，请先启动 backend_api_example.py"
echo "      然后创建 .env 文件并设置 VITE_USE_API=true"
echo ""

