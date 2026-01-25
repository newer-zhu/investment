import { useState, useEffect } from 'react'
import { Spin, Alert, Card } from 'antd'
import Papa from 'papaparse'
import StockTable from '../components/StockTable'
import { getFinalStocks } from '../utils/api'
import '../App.css'

// 配置：使用API模式还是静态文件模式
const USE_API = import.meta.env.VITE_USE_API === 'true' || false
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

function FinalStocks() {
  const [stocks, setStocks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // 加载最终选股数据
  useEffect(() => {
    setLoading(true)
    setError(null)

    if (USE_API) {
      // API模式：从后端获取JSON数据
      getFinalStocks()
        .then(data => {
          // 转换数据格式
          const formattedData = data.map((row, index) => ({
            id: index + 1,
            code: row['代码'] || row.code,
            name: row['名称'] || row.name,
            price: parseFloat(row['价格'] || row.price || 0),
            change: parseFloat(row['今日涨跌'] || row.change || 0),
            marketCap: parseFloat(row['总市值'] || row.marketCap || 0),
            ytdChange: parseFloat(row['年初至今涨跌幅'] || row.ytdChange || 0),
            industry: row['行业'] || row.industry,
            fundamentalScore: parseFloat(row['基本面评分'] || row.fundamentalScore || 0),
            technicalScore: parseFloat(row['技术面评分'] || row.technicalScore || 0),
            totalScore: parseFloat(row['总分'] || row.totalScore || 0),
          })).filter(stock => stock.code)
          
          setStocks(formattedData)
          setLoading(false)
        })
        .catch(err => {
          setError(`加载数据失败: ${err.message}`)
          setLoading(false)
        })
    } else {
      // 静态文件模式：从项目根目录的output目录加载CSV
      const csvPath = `/output/final_stocks.csv`
      
      console.log('[FinalStocks] Fetching CSV from:', csvPath)
      
      fetch(csvPath)
        .then(response => {
          console.log('[FinalStocks] Response status:', response.status, response.statusText)
          if (!response.ok) {
            throw new Error(`无法加载文件: ${response.status} ${response.statusText}`)
          }
          return response.text()
        })
        .then(text => {
          Papa.parse(text, {
            header: true,
            skipEmptyLines: true,
            complete: (results) => {
              // 转换数据格式
              const formattedData = results.data.map((row, index) => ({
                id: index + 1,
                code: row['代码'] || row.code,
                name: row['名称'] || row.name,
                price: parseFloat(row['价格'] || row.price || 0),
                change: parseFloat(row['今日涨跌'] || row.change || 0),
                marketCap: parseFloat(row['总市值'] || row.marketCap || 0),
                ytdChange: parseFloat(row['年初至今涨跌幅'] || row.ytdChange || 0),
                industry: row['行业'] || row.industry,
                fundamentalScore: parseFloat(row['基本面评分'] || row.fundamentalScore || 0),
                technicalScore: parseFloat(row['技术面评分'] || row.technicalScore || 0),
                totalScore: parseFloat(row['总分'] || row.totalScore || 0),
              })).filter(stock => stock.code) // 过滤空行
              
              setStocks(formattedData)
              setLoading(false)
            },
            error: (error) => {
              setError(`解析CSV时出错: ${error.message}`)
              setLoading(false)
            }
          })
        })
        .catch(err => {
          console.error('[FinalStocks] Fetch error:', err)
          setError(`加载数据失败: ${err.message}`)
          setLoading(false)
        })
    }
  }, [])

  return (
    <Card className="main-card">
          {loading && (
            <div className="loading-container">
              <Spin size="large" tip="加载中..." />
            </div>
          )}

          {error && (
            <Alert
              message="加载失败"
              description={
                <div>
                  <p>{error}</p>
                  <p style={{ marginTop: 8, fontSize: '12px', color: '#666' }}>
                    提示：请确保 final_stocks.csv 文件位于 <code>public/output/</code> 目录下，或配置后端API来提供数据
                  </p>
                </div>
              }
              type="error"
              showIcon
              style={{ marginBottom: 16 }}
            />
          )}

          {!loading && !error && stocks.length > 0 && (
            <StockTable stocks={stocks} />
          )}

          {!loading && !error && stocks.length === 0 && (
            <Alert
              message="暂无数据"
              description="当前没有最终选股数据"
              type="info"
              showIcon
            />
          )}
        </Card>
  )
}

export default FinalStocks

