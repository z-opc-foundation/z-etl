import {defineConfig} from 'vite'
import react from '@vitejs/plugin-react'

// 后端落点：本地 aio 的 z-etl（zopc-aio-zetl，8093 发布 8092）。
// /etl/ 前缀形态：请求 /api/** → 8093 的 /etl/api/**（剥前缀反代同口径）。
const ETL_ORIGIN = process.env.ETL_ORIGIN || 'http://127.0.0.1:8093'
const PORT = Number(process.env.SUIT_PORT || 5175)

export default defineConfig({
    plugins: [react()],
    server: {
        port: PORT,
        host: '0.0.0.0',
        proxy: {
            '^/api/': {
                target: ETL_ORIGIN,
                changeOrigin: true,
                rewrite: (p) => p.replace(/^\/api/, '/etl/api'),
            },
        },
    },
})
