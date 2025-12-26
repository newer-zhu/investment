/**
 * API工具函数
 * 用于与后端API通信
 */

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

/**
 * 获取所有可用的日期列表
 */
export async function getAvailableDates() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/dates`)
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }
    const data = await response.json()
    return data.dates || []
  } catch (error) {
    console.error('获取日期列表失败:', error)
    return []
  }
}

/**
 * 获取指定日期的股票数据
 */
export async function getStocksByDate(date) {
  try {
    const response = await fetch(`${API_BASE_URL}/api/stocks/${date}`)
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }
    const data = await response.json()
    return data.stocks || []
  } catch (error) {
    console.error(`获取日期 ${date} 的股票数据失败:`, error)
    throw error
  }
}

