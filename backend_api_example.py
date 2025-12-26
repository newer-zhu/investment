"""
后端API示例 - 用于为前端提供股票推荐数据
可以使用Flask或FastAPI实现，这里提供Flask示例

安装依赖：
pip install flask flask-cors pandas

运行：
python backend_api_example.py
"""

from flask import Flask, jsonify, send_file
from flask_cors import CORS
import os
import pandas as pd
from datetime import datetime

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# CSV文件目录
CSV_DIR = os.path.join(os.path.dirname(__file__), 'output')


@app.route('/api/stocks/<date>', methods=['GET'])
def get_stocks_by_date(date):
    """
    根据日期获取股票推荐数据
    
    参数:
        date: 日期字符串，格式 YYYYMMDD，如 20251225
    
    返回:
        JSON格式的股票数据列表
    """
    try:
        csv_file = os.path.join(CSV_DIR, f'picked_stocks_{date}.csv')
        
        if not os.path.exists(csv_file):
            return jsonify({
                'error': f'未找到日期 {date} 的数据',
                'available_dates': get_available_dates()
            }), 404
        
        # 读取CSV文件
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        
        # 转换为字典列表
        stocks = df.to_dict('records')
        
        return jsonify({
            'date': date,
            'count': len(stocks),
            'stocks': stocks
        })
    
    except Exception as e:
        return jsonify({
            'error': f'读取数据失败: {str(e)}'
        }), 500


@app.route('/api/dates', methods=['GET'])
def get_available_dates():
    """
    获取所有可用的日期列表
    
    返回:
        JSON格式的日期列表
    """
    dates = []
    if os.path.exists(CSV_DIR):
        for filename in os.listdir(CSV_DIR):
            if filename.startswith('picked_stocks_') and filename.endswith('.csv'):
                date_str = filename.replace('picked_stocks_', '').replace('.csv', '')
                dates.append(date_str)
    
    dates.sort(reverse=True)  # 最新的在前
    return jsonify({
        'dates': dates,
        'count': len(dates)
    })


@app.route('/api/stocks/<date>/csv', methods=['GET'])
def download_csv(date):
    """
    下载指定日期的CSV文件
    
    参数:
        date: 日期字符串，格式 YYYYMMDD
    
    返回:
        CSV文件
    """
    try:
        csv_file = os.path.join(CSV_DIR, f'picked_stocks_{date}.csv')
        
        if not os.path.exists(csv_file):
            return jsonify({'error': f'未找到日期 {date} 的数据'}), 404
        
        return send_file(
            csv_file,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'picked_stocks_{date}.csv'
        )
    
    except Exception as e:
        return jsonify({
            'error': f'下载文件失败: {str(e)}'
        }), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({
        'status': 'ok',
        'csv_dir': CSV_DIR,
        'csv_dir_exists': os.path.exists(CSV_DIR)
    })


if __name__ == '__main__':
    print(f'启动后端API服务器...')
    print(f'CSV目录: {CSV_DIR}')
    print(f'API地址: http://localhost:8000')
    print(f'可用接口:')
    print(f'  GET /api/dates - 获取可用日期列表')
    print(f'  GET /api/stocks/<date> - 获取指定日期的股票数据')
    print(f'  GET /api/stocks/<date>/csv - 下载CSV文件')
    print(f'  GET /api/health - 健康检查')
    
    app.run(host='0.0.0.0', port=8000, debug=True)

