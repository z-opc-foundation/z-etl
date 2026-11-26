/**
 * 无限画布编辑器（真源 = 控制台 PipelineCanvas，2026-11-26 组件化照搬）：
 * 点算子入画布自动网格排布 · 拖拽摆位（负坐标放开 + 边缘自动跟随）·
 * 端口拖连线 · 点连线删边 · 滚轮缩放 / 拖空白平移 · Delete 删选中节点。
 *
 * ed 状态放 ref（命令式口径，拖拽期间只动 transform 与边 path，不整树重渲）。
 */
import {useEffect, useRef, useState} from 'react'
import {apiGet, apiPost, errMsg} from '../services/api.js'
import {EXAMPLES, KIND_COLOR, KIND_STYLE, KIND_TAG, kindOf} from '../lib/operators.js'
import {esc} from '../lib/dag.js'

const ED_W = 210, ED_H = 64, ED_SVG_W = 4000, ED_SVG_H = 2400, ED_SVG_PAD = 2000, ED_DRAG_MIN = -1800

function blankEd(name) {
    return {srcName: null, name: name || '', nodes: [], edges: [], sel: null, zoom: 1, panX: 60, panY: 60, link: null, drag: null}
}

function specToEd(spec) {
    const nodes = (spec.nodes || []).map((n) => ({id: n.id, type: n.type, cfg: n.config && typeof n.config === 'object' ? n.config : {}, x: 0, y: 0}))
    const edges = (spec.edges || []).filter((e) => e && e.from !== e.to).map((e) => ({from: e.from, to: e.to}))
    const {columns} = layerColumns(nodes, edges)
    columns.forEach((col, L) => col.forEach((n, i) => { n.x = 60 + L * (ED_W + 130); n.y = 60 + i * (ED_H + 70) }))
    return {srcName: spec.name || null, name: spec.name || '', nodes, edges, sel: null, zoom: 1, panX: 40, panY: 40, link: null, drag: null}
}

function layerColumns(nodes, edges) {
    const layer = {}
    nodes.forEach((n) => { layer[n.id] = 0 })
    for (let i = 0; i < nodes.length; i++) {
        edges.forEach((e) => {
            if (layer[e.from] != null && layer[e.to] != null && layer[e.to] < layer[e.from] + 1) layer[e.to] = layer[e.from] + 1
        })
    }
    const cols = []
    nodes.forEach((n) => { const L = layer[n.id] || 0; (cols[L] = cols[L] || []).push(n) })
    while (cols.length && !cols[cols.length - 1]) cols.pop()
    return {layer, columns: cols}
}

function edgePathD(a, b) {
    const x1 = a.x + ED_W, y1 = a.y + ED_H / 2, x2 = b.x, y2 = b.y + ED_H / 2
    const dx = Math.max(50, (x2 - x1) / 2)
    return 'M ' + x1 + ' ' + y1 + ' C ' + (x1 + dx) + ' ' + y1 + ', ' + (x2 - dx) + ' ' + y2 + ', ' + x2 + ' ' + y2
}

/**
 * props: {spec: 编辑中的声明（null = 新建）, operators, notify, onSaved(name), onCancel}
 */
export default function PipelineCanvas({spec, operators, notify, onSaved, onCancel}) {
    const edRef = useRef(null)
    const [, setTick] = useState(0)
    const canvasRef = useRef(null)
    const worldRef = useRef(null)
    const svgRef = useRef(null)
    const edgesRef = useRef(null)
    const nameRef = useRef(null)
    const rerender = () => setTick((t) => t + 1)

    if (edRef.current === null) {
        edRef.current = spec ? specToEd(spec) : blankEd()
    }
    const ed = edRef.current

    const applyTransform = () => {
        const e = edRef.current
        if (worldRef.current) {
            worldRef.current.style.transform = 'translate(' + e.panX + 'px,' + e.panY + 'px) scale(' + e.zoom + ')'
        }
    }

    const renderEdges = () => {
        const e = edRef.current
        const g = edgesRef.current
        if (!g) return
        let h = ''
        e.edges.forEach((edge, i) => {
            const a = e.nodes.find((n) => n.id === edge.from)
            const b = e.nodes.find((n) => n.id === edge.to)
            if (!a || !b) return
            h += '<path class="zetl-edge" data-i="' + i + '" d="' + edgePathD(a, b) + '" fill="none" stroke="#9aa7b4" stroke-width="2" marker-end="url(#zetl-edarw)"/>'
        })
        if (e.link) h += '<path d="' + e.link.d + '" fill="none" stroke="#1677ff" stroke-width="2" stroke-dasharray="6 4"/>'
        g.innerHTML = h
        g.querySelectorAll('path.zetl-edge').forEach((p) => {
            p.onclick = (ev) => {
                ev.stopPropagation()
                e.edges.splice(Number(p.getAttribute('data-i')), 1)
                renderEdges()
                notify('已删除连线', true)
            }
        })
    }

    const edToWorld = (ev) => {
        const e = edRef.current
        const r = canvasRef.current.getBoundingClientRect()
        return {x: (ev.clientX - r.left - e.panX) / e.zoom, y: (ev.clientY - r.top - e.panY) / e.zoom}
    }

    const edAdd = (type) => {
        const op = (operators || []).find((o) => o.id === type)
        if (!op) { notify('未知算子：' + type, false); return }
        const base = type.split('.').pop()
        let id = base, n = 1
        while (ed.nodes.some((x) => x.id === id)) { n++; id = base + '_' + n }
        const r = canvasRef.current.getBoundingClientRect()
        const cx = (r.width / 2 - ed.panX) / ed.zoom - ED_W / 2
        const cy = (r.height / 2 - ed.panY) / ed.zoom - ED_H / 2
        const cols = Math.max(1, Math.floor((r.width - 48) / (ED_W + 48)))
        const x0 = cx - ((cols - 1) * (ED_W + 48)) / 2
        const k = ed.nodes.length
        const cfgSrc = EXAMPLES[type] != null ? EXAMPLES[type] : {}
        ed.nodes.push({
            id, type,
            cfg: JSON.parse(JSON.stringify(cfgSrc)),
            x: Math.max(8, Math.round(x0) + (k % cols) * (ED_W + 48)),
            y: Math.max(8, Math.round(cy) + Math.floor(k / cols) * (ED_H + 44)),
        })
        ed.sel = ed.nodes.length - 1
        rerender()
    }

    const edDelNode = () => {
        if (ed.sel == null) return
        const n = ed.nodes[ed.sel]
        ed.edges = ed.edges.filter((e) => e.from !== n.id && e.to !== n.id)
        ed.nodes.splice(ed.sel, 1)
        ed.sel = null
        rerender()
    }

    const edApplyCfg = () => {
        const n = ed.nodes[ed.sel]
        if (!n) return
        const newId = document.getElementById('zetl-ed-cfg-id').value.trim()
        if (!newId) { notify('节点 id 不能为空', false); return }
        const dup = ed.nodes.some((x, i) => i !== ed.sel && x.id === newId)
        if (dup) { notify('节点 id 重复：' + newId, false); return }
        let cfg
        try { cfg = JSON.parse(document.getElementById('zetl-ed-cfg-json').value || '{}') } catch (e2) { notify('config 不是合法 JSON：' + e2.message, false); return }
        if (newId !== n.id) ed.edges.forEach((e) => { if (e.from === n.id) e.from = newId; if (e.to === n.id) e.to = newId })
        n.id = newId
        n.cfg = cfg
        notify('节点配置已应用', true)
        rerender()
    }

    const edClear = () => {
        const name = nameRef.current ? nameRef.current.value.trim() : ''
        edRef.current = blankEd(name)
        rerender()
        notify('画布已清空', true)
    }

    const zoomAtCenter = (z2) => {
        const e = edRef.current
        const r = canvasRef.current.getBoundingClientRect()
        const mx = r.width / 2, my = r.height / 2
        e.panX = mx - (mx - e.panX) * (z2 / e.zoom)
        e.panY = my - (my - e.panY) * (z2 / e.zoom)
        e.zoom = z2
        applyTransform()
        rerender()
    }

    const edZoomStep = (k) => {
        const z2 = Math.min(2, Math.max(0.3, ed.zoom * k))
        zoomAtCenter(z2)
    }

    const edZoomFit = () => {
        const e = edRef.current
        if (!e.nodes.length) { e.zoom = 1; e.panX = 60; e.panY = 60 }
        else {
            let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity
            e.nodes.forEach((n) => {
                x0 = Math.min(x0, n.x); y0 = Math.min(y0, n.y)
                x1 = Math.max(x1, n.x + ED_W); y1 = Math.max(y1, n.y + ED_H)
            })
            const cr = canvasRef.current.getBoundingClientRect()
            const bw = Math.max(1, x1 - x0), bh = Math.max(1, y1 - y0)
            e.zoom = Math.min(1.2, Math.max(0.3, Math.min((cr.width - 60) / bw, (cr.height - 60) / bh)))
            e.panX = (cr.width - bw * e.zoom) / 2 - x0 * e.zoom
            e.panY = (cr.height - bh * e.zoom) / 2 - y0 * e.zoom
        }
        applyTransform()
        rerender()
    }

    // 打开即内容自适应（负坐标节点一并入画）；每次提交后同步边层与 transform
    // （边是命令式 innerHTML，React 不知道要重画）
    useEffect(() => { edZoomFit() }, [])
    useEffect(() => { renderEdges(); applyTransform() })

    const edSave = async () => {
        const name = nameRef.current ? nameRef.current.value.trim() : ''
        if (!name) { notify('先填模板名', false); return }
        if (!ed.nodes.length) { notify('画布上还没有节点——从左侧点算子入画布', false); return }
        const ids = ed.nodes.map((n) => n.id)
        for (let i = 0; i < ids.length; i++) {
            if (ids.indexOf(ids[i]) !== i) { notify('节点 id 重复：' + ids[i], false); return }
        }
        const out = {name, nodes: ed.nodes.map((n) => ({id: n.id, type: n.type, config: n.cfg})), edges: ed.edges}
        try {
            await apiPost('/etl/pipelines', JSON.stringify(out, null, 2), {raw: true})
            notify('模板已保存：' + name, true)
            if (onSaved) onSaved(name)
        } catch (e) { notify('保存失败：' + errMsg(e), false) }
    }

    // 命令式交互：mousemove / mouseup / keydown 挂 document（只挂一次，读 ref）
    useEffect(() => {
        const onMove = (ev) => {
            const e = edRef.current
            if (!e || !e.drag) return
            const d = e.drag
            const canvas = canvasRef.current
            if (d.mode === 'pan') {
                if (Math.abs(ev.clientX - d.cx) + Math.abs(ev.clientY - d.cy) > 4) d.moved = true
                e.panX = d.panX0 + (ev.clientX - d.cx)
                e.panY = d.panY0 + (ev.clientY - d.cy)
                applyTransform()
            } else if (d.mode === 'node') {
                if (Math.abs(ev.clientX - d.cx) + Math.abs(ev.clientY - d.cy) > 4) d.moved = true
                const n = e.nodes[d.i]
                if (!n) return
                n.x = Math.max(ED_DRAG_MIN, d.nx0 + (ev.clientX - d.cx) / e.zoom)
                n.y = Math.max(ED_DRAG_MIN, d.ny0 + (ev.clientY - d.cy) / e.zoom)
                const world = worldRef.current
                const el = world && world.querySelector('.zetl-ed-node[data-i="' + d.i + '"]')
                if (el) { el.style.left = n.x + 'px'; el.style.top = n.y + 'px' }
                renderEdges()
                // 拖近可视边界自动跟随平移（无限画布手感）
                const cr = canvas.getBoundingClientRect()
                const sx = n.x * e.zoom + e.panX, sy = n.y * e.zoom + e.panY
                const edge = 36
                let moved = false
                if (sy < edge) { e.panY += edge - sy; moved = true }
                if (sx < edge) { e.panX += edge - sx; moved = true }
                if (sy > cr.height - edge) { e.panY -= sy - (cr.height - edge); moved = true }
                if (sx > cr.width - edge) { e.panX -= sx - (cr.width - edge); moved = true }
                if (moved) applyTransform()
            } else if (d.mode === 'link') {
                const a = e.nodes[d.i]
                if (!a) return
                const w = edToWorld(ev)
                e.link = {d: 'M ' + (a.x + ED_W) + ' ' + (a.y + ED_H / 2) + ' L ' + w.x + ' ' + w.y}
                renderEdges()
            }
        }
        const onUp = (ev) => {
            const e = edRef.current
            if (!e || !e.drag) return
            const d = e.drag
            e.drag = null
            if (canvasRef.current) canvasRef.current.classList.remove('zetl-panning')
            if (d.mode === 'pan') {
                if (!d.moved && e.sel != null) { e.sel = null; rerender() }
            } else if (d.mode === 'node') {
                e.sel = d.i
                rerender()
            } else if (d.mode === 'link') {
                const svg = svgRef.current
                const prevVis = svg ? svg.style.visibility : ''
                if (svg) svg.style.visibility = 'hidden'
                const t = document.elementFromPoint(ev.clientX, ev.clientY)
                if (svg) svg.style.visibility = prevVis
                const target = t && t.closest ? t.closest('.zetl-port-in, .zetl-ed-node') : null
                e.link = null
                if (target) {
                    const j = Number(target.getAttribute('data-i'))
                    if (j !== d.i && e.nodes[d.i] && e.nodes[j]) {
                        const from = e.nodes[d.i], to = e.nodes[j]
                        if (kindOf(from.type) === 'sink') notify('汇算子不能再往外出线', false)
                        else if (kindOf(to.type) === 'source') notify('源算子不接受入边', false)
                        else if (e.edges.some((x) => x.from === from.id && x.to === to.id)) notify('这条边已存在', false)
                        else e.edges.push({from: from.id, to: to.id})
                    }
                }
                renderEdges()
            }
        }
        const onKey = (ev) => {
            const e = edRef.current
            if (!e) return
            if (ev.key !== 'Delete' && ev.key !== 'Backspace') return
            const t = ev.target
            if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA')) return
            if (e.sel != null) { ev.preventDefault(); edDelNode() }
        }
        document.addEventListener('mousemove', onMove)
        document.addEventListener('mouseup', onUp)
        document.addEventListener('keydown', onKey)
        return () => {
            document.removeEventListener('mousemove', onMove)
            document.removeEventListener('mouseup', onUp)
            document.removeEventListener('keydown', onKey)
        }
    }, [])

    const onCanvasMouseDown = (ev) => {
        const portEl = ev.target.closest ? ev.target.closest('.zetl-port') : null
        const nodeEl = ev.target.closest ? ev.target.closest('.zetl-ed-node') : null
        if (portEl) {
            ed.drag = {mode: 'link', i: Number(portEl.getAttribute('data-i'))}
            ev.preventDefault()
            return
        }
        if (nodeEl) {
            const i = Number(nodeEl.getAttribute('data-i'))
            ed.drag = {mode: 'node', i, cx: ev.clientX, cy: ev.clientY, nx0: ed.nodes[i].x, ny0: ed.nodes[i].y, moved: false}
            ev.preventDefault()
            return
        }
        ed.drag = {mode: 'pan', cx: ev.clientX, cy: ev.clientY, panX0: ed.panX, panY0: ed.panY, moved: false}
        canvasRef.current.classList.add('zetl-panning')
    }

    const onCanvasWheel = (ev) => {
        ev.preventDefault()
        const r = canvasRef.current.getBoundingClientRect()
        const mx = ev.clientX - r.left, my = ev.clientY - r.top
        const z2 = Math.min(2, Math.max(0.3, ed.zoom * (ev.deltaY < 0 ? 1.1 : 0.9)))
        ed.panX = mx - (mx - ed.panX) * (z2 / ed.zoom)
        ed.panY = my - (my - ed.panY) * (z2 / ed.zoom)
        ed.zoom = z2
        applyTransform()
        rerender()
    }

    const sel = ed.sel != null ? ed.nodes[ed.sel] : null
    const selKindKey = sel ? ({source: 'SOURCE', sink: 'SINK', stateful: 'STATEFUL'})[kindOf(sel.type)] || 'TRANSFORM' : null
    const selOp = sel ? (operators || []).find((o) => o.id === sel.type) : null

    return (
        <div className="zetl-panel zetl-editor">
            <div className="zetl-row" style={{alignItems: 'center'}}>
                <button className="zetl-act" onClick={onCancel}>← 返回列表</button>
                <input type="text" ref={nameRef} defaultValue={ed.name} placeholder="模板名（必填，如 ods-user-daily）" style={{width: 260, flex: 'none'}}/>
                <button className="zetl-act zetl-primary" onClick={edSave}>保存模板</button>
                <button className="zetl-act" onClick={edClear}>清空画布</button>
                <span style={{flex: 1}}/>
                <span className="zetl-zoombar" style={{margin: 0}}>
                    <span>缩放</span>
                    <button className="zetl-act" onClick={() => edZoomStep(0.9)}>−</button>
                    <span style={{minWidth: 44, textAlign: 'center'}}>{Math.round(ed.zoom * 100)}%</span>
                    <button className="zetl-act" onClick={() => edZoomStep(1.1)}>＋</button>
                    <button className="zetl-act" onClick={edZoomFit}>复位</button>
                </span>
            </div>
            <p style={{fontSize: 12, color: 'var(--zetl-dim)', margin: '4px 0 8px'}}>
                左侧点算子入画布 · 拖节点摆位 · 从右侧圆点拖到目标节点连线 · 点连线删边 · 点节点在右侧配 config · 滚轮缩放 / 拖空白平移 · Delete 删选中节点
            </p>
            <div className="zetl-ed-shell">
                <div className="zetl-ed-palette zetl-panel">
                    {!(operators && operators.length) && <p className="zetl-empty">算子目录未加载</p>}
                    {['SOURCE', 'TRANSFORM', 'STATEFUL', 'SINK'].map((kind) => (
                        <div key={kind}>
                            <div className="zetl-pal-group">{KIND_TAG[kind]}</div>
                            {(operators || []).filter((o) => o.kind === kind).map((o) => (
                                <div key={o.id} className="zetl-pal-item" title={o.desc || ''}
                                    style={{borderLeft: '3px solid ' + KIND_COLOR[kind]}}
                                    onClick={() => edAdd(o.id)}>{o.id}</div>
                            ))}
                        </div>
                    ))}
                </div>
                <div ref={canvasRef} className={'zetl-ed-canvas' + (ed.drag && ed.drag.mode === 'pan' ? ' zetl-panning' : '')}
                    onMouseDown={onCanvasMouseDown} onWheel={onCanvasWheel}>
                    <div ref={worldRef} className="zetl-ed-world">
                        <svg ref={svgRef} className="zetl-ed-svg" width={ED_SVG_W + ED_SVG_PAD} height={ED_SVG_H + ED_SVG_PAD}
                            viewBox={-ED_SVG_PAD + ' ' + -ED_SVG_PAD + ' ' + (ED_SVG_W + ED_SVG_PAD) + ' ' + (ED_SVG_H + ED_SVG_PAD)}>
                            <defs>
                                <marker id="zetl-edarw" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                                    <path d="M 0 0 L 10 5 L 0 10 z" fill="#9aa7b4"/>
                                </marker>
                            </defs>
                            {/* 边层：命令式 innerHTML 管理路径（React 只 owns defs，marker 定义不被冲掉） */}
                            <g ref={edgesRef}/>
                        </svg>
                        {ed.nodes.map((n, i) => {
                            const kind = kindOf(n.type)
                            return (
                                <div key={i} className={'zetl-ed-node' + (ed.sel === i ? ' sel' : '')} data-i={i}
                                    style={{left: n.x, top: n.y, borderLeftColor: KIND_STYLE[kind].stroke}}>
                                    <div className="zetl-ed-node-h">{esc(n.id)}</div>
                                    <div className="zetl-ed-node-t">{esc(n.type)}</div>
                                    {kind !== 'source' && <span className="zetl-port zetl-port-in" data-i={i}/>}
                                    {kind !== 'sink' && <span className="zetl-port zetl-port-out" data-i={i}/>}
                                </div>
                            )
                        })}
                    </div>
                </div>
                <div className="zetl-ed-cfg">
                    {!sel && <p className="zetl-empty">点选画布上的节点，在这里配 id 与 config。</p>}
                    {sel && (
                        <>
                            <label>节点 id</label>
                            <input type="text" id="zetl-ed-cfg-id" defaultValue={sel.id}/>
                            <label>算子类型 · {KIND_TAG[selKindKey]}</label>
                            <p style={{fontSize: 12, margin: 0}}><code>{sel.type}</code></p>
                            {selOp && selOp.desc && <p style={{fontSize: 11, color: 'var(--zetl-dim)', margin: '4px 0 0'}}>{selOp.desc}</p>}
                            <label>config（JSON）</label>
                            <textarea id="zetl-ed-cfg-json" style={{minHeight: 150}} defaultValue={JSON.stringify(sel.cfg, null, 2)}/>
                            <div className="zetl-row" style={{marginTop: 8}}>
                                <button className="zetl-act zetl-primary" onClick={edApplyCfg}>应用</button>
                                <button className="zetl-act" style={{color: 'var(--zetl-err)', borderColor: 'var(--zetl-err)'}} onClick={edDelNode}>删除节点</button>
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>
    )
}
