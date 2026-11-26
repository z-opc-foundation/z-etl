import {defineConfig} from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'
import {safeEmptyOutDir} from '../build-tools/safe-vite-plugin.mjs'

export default defineConfig({
    plugins: [react(), safeEmptyOutDir()],
    resolve: {
        alias: {'@': path.resolve(__dirname, 'src')},
        dedupe: ['react', 'react-dom', 'axios'],
    },
    build: {
        lib: {
            entry: {
                index: path.resolve(__dirname, 'src/index.js'),
                pages: path.resolve(__dirname, 'src/pages.js'),
            },
            formats: ['es'],
        },
        outDir: 'dist',
        emptyOutDir: false,
        rollupOptions: {
            // 宿主提供 react/react-dom，axios 是唯一运行依赖 —— 其余全部打进本包
            external: (id) =>
                id === 'react' ||
                id === 'react-dom' ||
                id === 'react-dom/client' ||
                id === 'react/jsx-runtime',
        },
        assetFileNames: (info) => (info.names && info.names[0] || info.name || '').endsWith('.css')
            ? 'z-etl-component.css'
            : '[name][extname]',
    },
})
