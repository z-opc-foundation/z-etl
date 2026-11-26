/**
 * DAG 拓扑与画布布局（最长路径分层 + 层内居中），SVG 字符串口径与控制台一致。
 */
import {kindOf, KIND_STYLE} from './operators.js'

export function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

export function fmtTime(ts) {
    if (ts == null || ts === '') return '—'
    const d = new Date(ts)
    const p = (n) => (n < 10 ? '0' : '') + n
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate())
        + ' ' + p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds())
}

/** 最长路径分层；返回 {layer, columns}。 */
export function layerNodes(nodes, edges) {
    const es = (edges || []).filter((e) => e && e.from !== e.to)
    const layer = {}
    nodes.forEach((n) => { layer[n.id] = 0 })
    for (let i = 0; i < nodes.length; i++) {
        es.forEach((e) => {
            if (layer[e.from] == null || layer[e.to] == null) return
            if (layer[e.to] < layer[e.from] + 1) layer[e.to] = layer[e.from] + 1
        })
    }
    const columns = []
    nodes.forEach((n) => {
        const L = layer[n.id] || 0
        ;(columns[L] = columns[L] || []).push(n)
    })
    while (columns.length && !columns[columns.length - 1]) columns.pop()
    return {layer, columns}
}

const NODE_W = 240, NODE_H = 76, GAP_X = 120, GAP_Y = 44, M = 20

/** 只读拓扑 SVG（分层布局 + 运行指标叠加 + 琥珀高亮），metrics 可为 null。 */
export function dagSvg(spec, metrics, selectedNode) {
    const nodes = spec.nodes || []
    const {columns} = layerNodes(nodes, spec.edges)
    if (!columns.length) return ''
    const maxRows = Math.max.apply(null, columns.map((c) => c.length))
    const W = M * 2 + columns.length * NODE_W + (columns.length - 1) * GAP_X
    const H = M * 2 + maxRows * NODE_H + (maxRows - 1) * GAP_Y
    const pos = {}
    columns.forEach((col, L) => {
        const colH = col.length * NODE_H + (col.length - 1) * GAP_Y
        col.forEach((n, i) => {
            pos[n.id] = {x: M + L * (NODE_W + GAP_X), y: M + (H - M * 2 - colH) / 2 + i * (NODE_H + GAP_Y)}
        })
    })
    let svg = '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%;min-width:720px;background:#fff;border:1px solid #d9d9d9;border-radius:8px;display:block">'
        + '<defs><marker id="arw" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        + '<path d="M 0 0 L 10 5 L 0 10 z" fill="#bfbfbf"/></marker></defs>'
    ;(spec.edges || []).filter((e) => e && e.from !== e.to).forEach((e) => {
        const a = pos[e.from], b = pos[e.to]
        if (!a || !b) return
        const x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2
        const dx = Math.max(48, (x2 - x1) / 2)
        svg += '<path d="M ' + x1 + ' ' + y1 + ' C ' + (x1 + dx) + ' ' + y1 + ', ' + (x2 - dx) + ' ' + y2 + ', ' + x2 + ' ' + y2
            + '" fill="none" stroke="#bfbfbf" stroke-width="2" marker-end="url(#arw)"/>'
    })
    nodes.forEach((n) => {
        const p = pos[n.id]
        const st = KIND_STYLE[kindOf(n.type)]
        const m = metrics ? metrics[n.id] : null
        const dropped = m && m['records-in'] != null && m['records-out'] != null && m['records-out'] < m['records-in']
        const on = n.id === selectedNode
        const stroke = dropped ? '#d48806' : st.stroke
        const badge = m
            ? '<text x="' + (p.x + NODE_W - 8) + '" y="' + (p.y + NODE_H - 9) + '" text-anchor="end" font-size="12" fill="#8c8c8c">'
            + esc('in ' + (m['records-in'] == null ? '—' : m['records-in']) + ' · out ' + (m['records-out'] == null ? '—' : m['records-out'])) + '</text>'
            : ''
        const tip = m ? Object.keys(m).map((k) => esc(k) + '=' + esc(m[k])).join('\n') : '尚无运行指标'
        svg += '<g class="zetl-dag-node" data-node="' + esc(n.id) + '" role="button"><title>' + tip + '</title>'
            + '<rect x="' + p.x + '" y="' + p.y + '" width="' + NODE_W + '" height="' + NODE_H + '" rx="10" '
            + 'fill="' + (on ? '#e6f4ff' : '#fff') + '" stroke="' + stroke + '" stroke-width="' + (on ? 3 : 2) + '"/>'
            + '<text x="' + (p.x + 12) + '" y="' + (p.y + 28) + '" font-size="15" font-weight="600" fill="rgba(0,0,0,.88)">' + esc(n.id) + '</text>'
            + '<text x="' + (p.x + 12) + '" y="' + (p.y + 49) + '" font-size="12.5" fill="' + st.stroke + '">' + esc(n.type) + '</text>'
            + badge + '</g>'
    })
    return svg + '</svg>'
}
