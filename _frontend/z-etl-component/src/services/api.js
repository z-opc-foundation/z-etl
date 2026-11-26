/**
 * z-etl 请求实例（axios，同源 cookie 自动携带）。
 *
 * baseURL 默认 '/api'（独立跑 8092 控制台形态）；
 * 嵌主壳时宿主调 {@link configureEtl}({apiBase: '/etl/api'})——
 * /etl/ 前缀由 vite/nginx 剥前缀反代到本服务，路径段不变。
 */
import axios from 'axios'

const request = axios.create({baseURL: '/api'})

export function configureEtl(config) {
    if (config && config.apiBase !== undefined) {
        request.defaults.baseURL = config.apiBase
    }
}

export function errMsg(e) {
    const d = e && e.response && e.response.data
    if (d && typeof d === 'object') {
        if (d.message) return 'HTTP ' + e.response.status + '：' + d.message
        if (d.error) return 'HTTP ' + e.response.status + '：' + d.error
        return 'HTTP ' + e.response.status + '：' + JSON.stringify(d).slice(0, 300)
    }
    if (d) return 'HTTP ' + (e.response ? e.response.status : '?') + '：' + String(d).slice(0, 300)
    return (e && e.message) || String(e)
}

export async function apiGet(path) {
    const r = await request.get(path)
    return r.data
}

/** 404 也正常返回（status 字段区分）：水位 / 调度这类「没登记过」的查询。 */
export async function apiGetMaybe404(path) {
    const r = await request.get(path, {validateStatus: (s) => (s >= 200 && s < 300) || s === 404})
    return r.data === '' || r.data == null ? null : r.data
}

/** 纯文本（计划解释）。 */
export async function apiText(path) {
    const r = await request.get(path, {responseType: 'text', validateStatus: () => true})
    if (r.status >= 300) throw new Error('HTTP ' + r.status)
    return typeof r.data === 'string' ? r.data : JSON.stringify(r.data, null, 2)
}

/** body = JSON 或原文（raw = yml/JSON 声明原文，text/plain）。 */
export async function apiPost(path, body, {raw} = {}) {
    const r = raw
        ? await request.post(path, body, {headers: {'Content-Type': 'text/plain'}})
        : await request.post(path, body)
    return r.data
}

export async function apiDelete(path) {
    const r = await request.delete(path)
    return r.data
}
