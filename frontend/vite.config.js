import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'
import { fileURLToPath } from 'url'
import fs from 'fs'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// 自定义插件：提供 output 目录的文件服务
function outputFileServer() {
  return {
    name: 'output-file-server',
    configureServer(server) {
      // 处理 /api/dates 请求，返回所有可用的日期列表
      server.middlewares.use((req, res, next) => {
        if (req.url === '/api/dates' || req.url.startsWith('/api/dates?')) {
          try {
            const outputDir = path.join(__dirname, '..', 'output')
            const dates = []
            
            if (fs.existsSync(outputDir)) {
              const files = fs.readdirSync(outputDir)
              for (const filename of files) {
                if (filename.startsWith('picked_stocks_') && filename.endsWith('.csv')) {
                  const dateStr = filename.replace('picked_stocks_', '').replace('.csv', '')
                  // 验证日期格式（8位数字）
                  if (/^\d{8}$/.test(dateStr)) {
                    dates.push(dateStr)
                  }
                }
              }
            }
            
            dates.sort((a, b) => b.localeCompare(a)) // 最新的在前
            
            res.setHeader('Content-Type', 'application/json')
            res.setHeader('Access-Control-Allow-Origin', '*')
            res.end(JSON.stringify({
              dates: dates,
              count: dates.length
            }))
            return
          } catch (error) {
            console.error('[Output File Server] Error getting dates:', error)
            res.statusCode = 500
            res.setHeader('Content-Type', 'application/json')
            res.end(JSON.stringify({ error: 'Internal Server Error' }))
            return
          }
        }
        next()
      })
      
      // 处理 /output 路径的请求
      server.middlewares.use((req, res, next) => {
        // 只处理 /output 路径的请求
        if (req.url && req.url.startsWith('/output/')) {
          try {
            // 获取请求路径，移除查询参数
            let urlPath = req.url
            if (urlPath.includes('?')) {
              urlPath = urlPath.split('?')[0]
            }
            
            // 移除 /output 前缀，获取文件名
            const fileName = urlPath.replace(/^\/output\//, '')
            if (!fileName) {
              res.statusCode = 400
              res.end('Bad Request: filename required')
              return
            }
            
            const filePath = path.join(__dirname, '..', 'output', fileName)
            const outputDir = path.join(__dirname, '..', 'output')
            
            // 安全检查：确保路径在 output 目录内
            const resolvedPath = path.resolve(filePath)
            const resolvedOutputDir = path.resolve(outputDir)
            
            console.log(`[Output File Server] Request: ${req.url}`)
            console.log(`[Output File Server] File path: ${filePath}`)
            console.log(`[Output File Server] Output dir: ${outputDir}`)
            console.log(`[Output File Server] File exists: ${fs.existsSync(filePath)}`)
            
            if (!resolvedPath.startsWith(resolvedOutputDir)) {
              console.log(`[Output File Server] Security check failed`)
              res.statusCode = 403
              res.end('Forbidden')
              return
            }
            
            // 检查文件是否存在
            if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
              try {
                const content = fs.readFileSync(filePath, 'utf-8')
                res.setHeader('Content-Type', 'text/csv; charset=utf-8')
                res.setHeader('Access-Control-Allow-Origin', '*')
                res.setHeader('Cache-Control', 'no-cache')
                console.log(`[Output File Server] Serving file: ${fileName} (${content.length} bytes)`)
                res.end(content)
                return // 重要：阻止继续处理
              } catch (error) {
                console.error('[Output File Server] Error reading file:', error)
                res.statusCode = 500
                res.end('Internal Server Error')
                return
              }
            } else {
              console.log(`[Output File Server] File not found: ${filePath}`)
              res.statusCode = 404
              res.end('File not found: ' + fileName)
              return // 重要：阻止继续处理
            }
          } catch (error) {
            console.error('[Output File Server] Error:', error)
            res.statusCode = 500
            res.end('Internal Server Error')
            return
          }
        }
        // 如果不是 /output 路径，继续下一个中间件
        next()
      })
    }
  }
}

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react(),
    outputFileServer() // 添加自定义插件
  ],
  server: {
    port: 3000,
    open: true,
    fs: {
      // 允许访问项目根目录
      allow: ['..']
    }
  }
})
