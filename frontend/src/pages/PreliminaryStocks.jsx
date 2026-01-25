import { useState, useEffect } from 'react'
import { Typography, Select, Spin, Alert, Card } from 'antd'
import Papa from 'papaparse'
import StockTable from '../components/StockTable'
import { getAvailableDates, getStocksByDate } from '../utils/api'
import '../App.css'

const { Text } = Typography

// 配置：使用API模式还是静态文件模式
const USE_API = import.meta.env.VITE_USE_API === 'true' || false
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

function PreliminaryStocks() {
  const [stocks, setStocks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedDate, setSelectedDate] = useState('')
  const [availableDates, setAvailableDates] = useState([])

  // 获取可用的日期列表
  useEffect(() => {
    if (USE_API) {
      // 从API获取日期列表
      getAvailableDates().then(dates => {
        setAvailableDates(dates)
        if (dates.length > 0) {
          setSelectedDate(dates[0])
        }
      }).catch(err => {
        console.error('获取日期列表失败:', err)
        setError('无法连接到后端API，请检查后端服务是否运行')
      })
    } else {
      // 静态模式：从 /api/dates 获取日期列表（通过 Vite 中间件）
      fetch('/api/dates')
        .then(response => {
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`)
          }
          return response.json()
        })
        .then(data => {
          const dates = data.dates || []
          console.log('[PreliminaryStocks] Available dates:', dates)
          setAvailableDates(dates)
          if (dates.length > 0) {
            setSelectedDate(dates[0])
          }
        })
        .catch(err => {
          console.error('获取日期列表失败:', err)
          setError('无法获取日期列表，请检查 output 目录')
        })
    }
  }, [])

  // 加载CSV数据
  useEffect(() => {
    if (!selectedDate) return

    setLoading(true)
    setError(null)

    if (USE_API) {
      // API模式：从后端获取JSON数据
      getStocksByDate(selectedDate)
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
      const csvPath = `/output/picked_stocks_${selectedDate}.csv`
      
      console.log('[PreliminaryStocks] Fetching CSV from:', csvPath)
      
      fetch(csvPath)
        .then(response => {
          console.log('[PreliminaryStocks] Response status:', response.status, response.statusText)
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
          console.error('[PreliminaryStocks] Fetch error:', err)
          setError(`加载数据失败: ${err.message}`)
          setLoading(false)
        })
    }
  }, [selectedDate])

  const dateOptions = availableDates.map(date => ({
    label: `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`,
    value: date
  }))

  return (
    <Card className="main-card">
          <div className="date-selector">
            <Text strong style={{ marginRight: 8 }}>选择日期：</Text>
            <Select
              value={selectedDate}
              onChange={setSelectedDate}
              options={dateOptions}
              style={{ width: 200 }}
              placeholder="请选择日期"
            />
          </div>

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
                    提示：请确保CSV文件位于 <code>public/output/</code> 目录下，或配置后端API来提供数据
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
              description="当前日期没有股票数据"
              type="info"
              showIcon
            />
          )}
        </Card>
  )
}

export default PreliminaryStocks

