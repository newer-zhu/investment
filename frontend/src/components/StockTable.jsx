import { useState, useMemo } from 'react'
import { Table, Select, InputNumber, Space, Typography, Tag } from 'antd'
import './StockTable.css'

const { Text } = Typography

function StockTable({ stocks }) {
  const [filterIndustry, setFilterIndustry] = useState('')
  const [filterMinScore, setFilterMinScore] = useState(0)

  // 获取所有行业
  const industries = useMemo(() => {
    const industrySet = new Set(stocks.map(s => s.industry).filter(Boolean))
    return Array.from(industrySet).sort()
  }, [stocks])

  // 过滤数据
  const filteredStocks = useMemo(() => {
    return stocks.filter(stock => {
      const industryMatch = !filterIndustry || stock.industry === filterIndustry
      const scoreMatch = stock.totalScore >= filterMinScore
      return industryMatch && scoreMatch
    })
  }, [stocks, filterIndustry, filterMinScore])

  const formatNumber = (num) => {
    if (num >= 100000000) {
      return (num / 100000000).toFixed(2) + '亿'
    } else if (num >= 10000) {
      return (num / 10000).toFixed(2) + '万'
    }
    return num.toFixed(2)
  }

  const formatPercent = (num) => {
    return (num >= 0 ? '+' : '') + num.toFixed(2) + '%'
  }

  const industryOptions = [
    { label: '全部行业', value: '' },
    ...industries.map(industry => ({ label: industry, value: industry }))
  ]

  const columns = [
    {
      title: '代码',
      dataIndex: 'code',
      key: 'code',
      width: 100,
      fixed: 'left',
      render: (text) => <Text code>{text}</Text>,
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 120,
      fixed: 'left',
    },
    {
      title: '价格',
      dataIndex: 'price',
      key: 'price',
      width: 100,
      sorter: (a, b) => a.price - b.price,
      render: (price) => `¥${price.toFixed(2)}`,
    },
    {
      title: '今日涨跌',
      dataIndex: 'change',
      key: 'change',
      width: 110,
      sorter: (a, b) => a.change - b.change,
      render: (change) => {
        const color = change > 0 ? '#ff4d4f' : change < 0 ? '#52c41a' : '#999'
        return <Text style={{ color, fontWeight: 'bold' }}>{formatPercent(change)}</Text>
      },
    },
    {
      title: '总市值',
      dataIndex: 'marketCap',
      key: 'marketCap',
      width: 120,
      sorter: (a, b) => a.marketCap - b.marketCap,
      render: (marketCap) => formatNumber(marketCap),
    },
    {
      title: '年初至今',
      dataIndex: 'ytdChange',
      key: 'ytdChange',
      width: 110,
      sorter: (a, b) => a.ytdChange - b.ytdChange,
      render: (ytdChange) => {
        const color = ytdChange > 0 ? '#ff4d4f' : ytdChange < 0 ? '#52c41a' : '#999'
        return <Text style={{ color, fontWeight: 'bold' }}>{formatPercent(ytdChange)}</Text>
      },
    },
    {
      title: '行业',
      dataIndex: 'industry',
      key: 'industry',
      width: 120,
      sorter: (a, b) => a.industry.localeCompare(b.industry),
      render: (industry) => <Tag color="blue">{industry}</Tag>,
    },
    {
      title: '基本面',
      dataIndex: 'fundamentalScore',
      key: 'fundamentalScore',
      width: 100,
      sorter: (a, b) => a.fundamentalScore - b.fundamentalScore,
      render: (score) => score.toFixed(1),
    },
    {
      title: '技术面',
      dataIndex: 'technicalScore',
      key: 'technicalScore',
      width: 100,
      sorter: (a, b) => a.technicalScore - b.technicalScore,
      render: (score) => score.toFixed(1),
    },
    {
      title: '总分',
      dataIndex: 'totalScore',
      key: 'totalScore',
      width: 100,
      sorter: (a, b) => a.totalScore - b.totalScore,
      defaultSortOrder: 'descend',
      render: (score) => (
        <Text strong style={{ color: '#1890ff', fontSize: '16px' }}>
          {score.toFixed(1)}
        </Text>
      ),
    },
  ]

  return (
    <div className="stock-table-container">
      <Space className="table-filters" size="middle" wrap>
        <Space>
          <Text strong>行业筛选：</Text>
          <Select
            value={filterIndustry}
            onChange={setFilterIndustry}
            options={industryOptions}
            style={{ width: 150 }}
            placeholder="选择行业"
          />
        </Space>

        <Space>
          <Text strong>最低总分：</Text>
          <InputNumber
            min={0}
            max={100}
            value={filterMinScore}
            onChange={(value) => setFilterMinScore(value || 0)}
            style={{ width: 100 }}
          />
        </Space>

        <Text strong style={{ color: '#1890ff', fontSize: '16px' }}>
          共 {filteredStocks.length} 只股票
        </Text>
      </Space>

      <Table
        columns={columns}
        dataSource={filteredStocks}
        rowKey="id"
        scroll={{ x: 1200 }}
        pagination={{
          pageSize: 20,
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `共 ${total} 条`,
        }}
        size="middle"
      />
    </div>
  )
}

export default StockTable

