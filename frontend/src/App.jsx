import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom'
import { Layout, Menu, Typography } from 'antd'
import { StockOutlined, CheckCircleOutlined, LineChartOutlined } from '@ant-design/icons'
import PreliminaryStocks from './pages/PreliminaryStocks'
import FinalStocks from './pages/FinalStocks'
import './App.css'

const { Header, Content } = Layout
const { Text } = Typography

function App() {
  return (
    <Router>
      <AppLayout />
    </Router>
  )
}

function AppLayout() {
  const location = useLocation()
  
  // 根据当前路径确定选中的菜单项
  const selectedKey = location.pathname === '/final' ? 'final' : 'preliminary'

  return (
    <Layout className="app-layout">
      <Header className="app-header">
        <div className="header-content">
          <div className="logo-section">
            <LineChartOutlined className="logo-icon" />
            <div className="logo-text">
              <Text className="system-name">红多量化</Text>
              <Text className="system-subtitle">Quantitative Trading System</Text>
            </div>
          </div>
          <Menu
            theme="dark"
            mode="horizontal"
            selectedKeys={[selectedKey]}
            className="main-menu"
          >
            <Menu.Item key="preliminary" icon={<StockOutlined />}>
              <Link to="/">初步选股结果</Link>
            </Menu.Item>
            <Menu.Item key="final" icon={<CheckCircleOutlined />}>
              <Link to="/final">最终选股结果</Link>
            </Menu.Item>
          </Menu>
        </div>
      </Header>

      <Content className="app-content">
        <Routes>
          <Route path="/" element={<PreliminaryStocks />} />
          <Route path="/final" element={<FinalStocks />} />
        </Routes>
      </Content>
    </Layout>
  )
}

export default App
