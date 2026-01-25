import React from 'react'
import ReactDOM from 'react-dom/client'
import { ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import App from './App.jsx'
import './index.css'

// 量化系统主题配置
const theme = {
  token: {
    colorPrimary: '#1890ff',
    colorSuccess: '#52c41a',
    colorWarning: '#faad14',
    colorError: '#ff4d4f',
    colorBgBase: '#1a1f3a',
    colorBgContainer: '#1a1f3a',
    colorBgElevated: '#252b4a',
    colorBorder: '#2d3748',
    colorText: '#ffffff',
    colorTextSecondary: '#a0aec0',
    borderRadius: 8,
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
  },
  components: {
    Table: {
      headerBg: 'rgba(26, 31, 58, 0.8)',
      headerColor: '#ffffff',
      rowHoverBg: 'rgba(24, 144, 255, 0.1)',
      borderColor: '#2d3748',
    },
    Card: {
      headerBg: 'rgba(26, 31, 58, 0.5)',
      actionsBg: 'rgba(26, 31, 58, 0.3)',
    },
    Menu: {
      itemBg: 'transparent',
      itemHoverBg: 'rgba(24, 144, 255, 0.1)',
      itemSelectedBg: 'rgba(24, 144, 255, 0.2)',
      itemActiveBg: 'rgba(24, 144, 255, 0.15)',
    },
    Select: {
      optionSelectedBg: 'rgba(24, 144, 255, 0.2)',
    },
  },
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN} theme={theme}>
      <App />
    </ConfigProvider>
  </React.StrictMode>,
)

