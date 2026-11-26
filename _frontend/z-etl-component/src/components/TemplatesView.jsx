import {useEffect, useState} from 'react'
import {apiGet, apiGetMaybe404, apiPost, apiText, apiDelete, errMsg} from '../services/api.js'
import {DEMO_TEXT, KIND_STYLE} from '../lib/operators.js'
import {dagSvg, fmtTime} from '../lib/dag.js'
import RunResult from './RunResult.jsx'
import PipelineCanvas from './PipelineCanvas.jsx'
import {useToast} from './useToast.jsx'

/** 只读 DAG 拓扑（缩放 + 点节点看全量指标）。 */
function DagView({spec, metrics}) {
    const [zoom, setZoom] = useState('fit')
    const [sel, setSel] = useState(null)
    const zooms = [['fit', '适应'], ['1', '1:1'], ['1.5', '150%'], ['2', '200%']]
    const nodeKeys = (spec.nodes || []).map((n) => n.id)
    const selValid = sel && nodeKeys.indexOf(sel) >= 0 ? sel : null
    let svg = dagSvg(spec, zoom === 'fit' ? metrics : metrics, selValid)
    if (zoom !== 'fit') {
        svg = svg.replace('style="width:100%;min-width:720px', 'style="width:' + Math.round(900 * Number(zoom)) + 'px;min-width:720px')
    }
    return (
        <div>
            <div className="zetl-zoombar">
                <span>缩放</span>
                {zooms.map((z) => (
                    <button key={z[0]} className={'zetl-act' + (String(zoom) === z[0] ? ' zetl-on' : '')}
                        onClick={() => setZoom(z[0] === 'fit' ? 'fit' : Number(z[0]))}>{z[1]}</button>
                ))}
            </div>
            <div
                className="zetl-dag-wrap"
                onClick={(ev) => {
                    const g = ev.target && ev.target.closest ? ev.target.closest('[data-node]') : null
                    if (!g) return
                    const id = g.getAttribute('data-node')
                    setSel(id === sel ? null : id)
                }}
                dangerouslySetInnerHTML={{__html: svg}}
            />
            <div className="zetl-dag-legend">
                {Object.keys(KIND_STYLE).map((k) => (
                    <span key={k} className="zetl-badge" style={{borderColor: KIND_STYLE[k].stroke}}>{KIND_STYLE[k].tag}</span>
                ))}
                <span className="zetl-badge" style={{borderColor: 'var(--zetl-warn)'}}>行数变化</span>
            </div>
            <p style={{fontSize: 11, color: 'var(--zetl-dim)'}}>
                边框色 = 算子类别（绿 源 / 灰 变换 / 蓝 有状态 / 橙 汇）；琥珀色节点 = 进出不等（行数被改写）；点击节点在下方看全量计数，运行 / 试跑后自动叠加最新指标。
            </p>
            {selValid && (
                <div>
                    <h3 className="zetl-sec">节点指标 · {selValid}</h3>
                    {metrics && metrics[selValid]
                        ? <pre>{JSON.stringify(metrics[selValid], null, 2)}</pre>
                        : <p className="zetl-empty">该节点暂无运行指标——运行 / 试跑后自动叠加。</p>}
                </div>
            )}
        </div>
    )
}

/**
 * 作业模板视图：列表 → 详情（拓扑 / 定义 / 计划 / 履历 / 水位 / 保存点 / 调度 / 运行）→ 无限画布编辑器。
 * props: {selected, onSelect, notify, toastEl, operators, onCount}
 */
export default function TemplatesView({selected, onSelect, notify, toastEl, operators, onCount}) {
    const [names, setNames] = useState([])
    const [mode, setMode] = useState('list') // list | detail | editor
    const [spec, setSpec] = useState(null) // 模板声明缓存
    const [metrics, setMetrics] = useState(null)
    const [body, setBody] = useState(null) // {title, node}
    const [extra, setExtra] = useState(null) // {title, node}
    const [runParams, setRunParams] = useState('')
    const [editorSpec, setEditorSpec] = useState(undefined) // undefined=关, null=新建, spec=编辑
    const name = selected || null

    useEffect(() => {
        refreshPipelines()
    }, [])

    const refreshPipelines = async () => {
        try {
            const data = await apiGet('/etl/pipelines')
            const list = data.pipelines || []
            setNames(list)
            if (onCount) onCount(list.length)
            return list
        } catch (e) {
            notify('模板列表加载失败：' + errMsg(e), false)
            return []
        }
    }

    const openDetail = async (n) => {
        onSelect(n)
        setSpec(null); setMetrics(null); setBody(null); setExtra(null); setRunParams('')
        setMode('detail')
        refreshPipelines()
        // 默认呈现拓扑
        try {
            const s = await apiGet('/etl/pipelines/' + encodeURIComponent(n))
            setSpec(s)
            let m = null
            try {
                const h = await apiGet('/etl/pipelines/' + encodeURIComponent(n) + '/runs?limit=1')
                if (h.runs && h.runs.length) m = h.runs[0].nodeMetrics || null
            } catch (e2) { /* 无履历就空着画 */ }
            setMetrics(m)
            setBody({title: '拓扑', node: <DagView spec={s} metrics={m}/>})
        } catch (e) {
            notify(errMsg(e), false)
            setBody({title: '拓扑', node: <p style={{color: 'var(--zetl-err)'}}>拓扑加载失败：{errMsg(e)}</p>})
        }
    }

    const showDag = async () => {
        if (!name) { notify('先选一个模板', false); return }
        setBody({title: '拓扑', node: <p className="zetl-empty">拓扑加载中…</p>})
        try {
            const s = spec && spec.name === name ? spec : await apiGet('/etl/pipelines/' + encodeURIComponent(name))
            setSpec(s)
            setBody({title: '拓扑', node: <DagView spec={s} metrics={metrics}/>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const showDef = async () => {
        if (!name) { notify('先选一个模板', false); return }
        try {
            const s = spec && spec.name === name ? spec : await apiGet('/etl/pipelines/' + encodeURIComponent(name))
            setSpec(s)
            setExtra(null)
            setBody({title: '定义', node: <pre>{JSON.stringify(s, null, 2)}</pre>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const showPlan = async () => {
        if (!name) { notify('先选一个模板', false); return }
        try {
            const text = await apiText('/etl/pipelines/' + encodeURIComponent(name) + '/plan')
            setExtra(null)
            setBody({title: '计划解释', node: <pre>{text}</pre>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const showRuns = async () => {
        if (!name) { notify('先选一个模板', false); return }
        try {
            const data = await apiGet('/etl/pipelines/' + encodeURIComponent(name) + '/runs?limit=20')
            const runs = data.runs || []
            setExtra(null)
            setBody({
                title: '运行履历（最近 ' + runs.length + ' 次）',
                node: runs.length
                    ? (
                        <div>
                            {runs.map((r, i) => (
                                <details key={i} open={i === 0}>
                                    <summary style={{cursor: 'pointer', padding: '4px 0'}}>
                                        {fmtTime(r.startedAt)} · {r.mode} · 读 {r.recordsRead} 写 {r.recordsWritten}
                                    </summary>
                                    <RunResult result={r}/>
                                </details>
                            ))}
                        </div>
                    )
                    : <p style={{color: 'var(--zetl-dim)'}}>还没有运行记录</p>,
            })
        } catch (e) { notify(errMsg(e), false) }
    }

    const showWatermark = async () => {
        if (!name) { notify('先选一个模板', false); return }
        try {
            const wm = await apiGetMaybe404('/etl/pipelines/' + encodeURIComponent(name) + '/watermark')
            setExtra(null)
            setBody({title: '水位记录', node: wm ? <pre>{JSON.stringify(wm, null, 2)}</pre> : <p style={{color: 'var(--zetl-dim)'}}>还没有水位（没跑过增量）</p>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const exportSavepoint = async () => {
        if (!name) { notify('先选一个模板', false); return }
        try {
            const sp = await apiGet('/etl/pipelines/' + encodeURIComponent(name) + '/savepoint')
            setExtra({
                title: '保存点报文（复制保存，灌回用）',
                node: (
                    <>
                        <textarea id="zetl-sp-json" style={{minHeight: 220}} defaultValue={JSON.stringify(sp, null, 2)}/>
                        <div className="zetl-row" style={{marginTop: 8}}>
                            <button className="zetl-act" onClick={() => {
                                const el = document.getElementById('zetl-sp-json')
                                el.select()
                                document.execCommand('copy')
                                notify('已复制', true)
                            }}>复制</button>
                        </div>
                    </>
                ),
            })
        } catch (e) { notify(errMsg(e), false) }
    }

    const showSchedPanel = async () => {
        if (!name) { notify('先选一个模板', false); return }
        try {
            const cur = await apiGetMaybe404('/etl/pipelines/' + encodeURIComponent(name) + '/schedule')
            setExtra({
                title: '调度登记',
                node: (
                    <>
                        <p style={{fontSize: 12, color: 'var(--zetl-dim)'}}>
                            {cur
                                ? <>当前：cron <code>{cur.cronExpression}</code>，{cur.enabled ? '已启用' : '已停用'}</>
                                : '未登记'}
                        </p>
                        <div className="zetl-row">
                            <input type="text" id="zetl-sched-cron" placeholder="cron 表达式（分 时 日 月 周），如 0 0 2 * * ?" style={{flex: 1, minWidth: 200}}/>
                            <button className="zetl-act zetl-primary" onClick={async () => {
                                try {
                                    const cron = document.getElementById('zetl-sched-cron').value.trim()
                                    const r = await apiPost('/etl/pipelines/' + encodeURIComponent(name) + '/schedule?enabled=true', cron, {raw: true})
                                    notify('调度已登记，下次触发：' + (r.nextFireTime || '—'), true)
                                    showSchedPanel()
                                } catch (e2) { notify(errMsg(e2), false) }
                            }}>保存</button>
                            {cur && (
                                <button className="zetl-act" onClick={async () => {
                                    try {
                                        await apiDelete('/etl/pipelines/' + encodeURIComponent(name) + '/schedule')
                                        notify('调度已取消', true)
                                        showSchedPanel()
                                    } catch (e2) { notify(errMsg(e2), false) }
                                }}>取消调度</button>
                            )}
                        </div>
                    </>
                ),
            })
        } catch (e) { notify(errMsg(e), false) }
    }

    const runPipe = async (dry) => {
        if (!name) { notify('先选一个模板', false); return }
        let bodyJson
        if (runParams.trim()) {
            try { bodyJson = JSON.parse(runParams) } catch (e) { notify('运行参数不是合法 JSON：' + e.message, false); return }
        }
        const path = '/etl/pipelines/' + encodeURIComponent(name) + (dry ? '/dry-run?rows=100' : '/run')
        try {
            const r = await apiPost(path, bodyJson)
            const m = r.nodeMetrics || null
            setMetrics(m)
            if (spec && spec.name === name) {
                setBody({
                    title: '拓扑（本轮指标已叠加）',
                    node: (
                        <>
                            <DagView spec={spec} metrics={m}/>
                            <h3 className="zetl-sec">{dry ? '试跑结果（不落地）' : '运行结果'}</h3>
                            <RunResult result={r}/>
                        </>
                    ),
                })
            } else {
                setBody({title: dry ? '试跑结果（不落地）' : '运行结果', node: <RunResult result={r}/>})
            }
        } catch (e) { notify(errMsg(e), false) }
    }

    const registerSpec = async () => {
        try {
            const el = document.getElementById('zetl-spec')
            const data = await apiPost('/etl/pipelines', el.value, {raw: true})
            notify('已登记：' + data.name, true)
            onSelect(data.name)
            await refreshPipelines()
        } catch (e) { notify(errMsg(e), false) }
    }

    // ── 画布编辑器 ──
    if (mode === 'editor') {
        return (
            <PipelineCanvas
                spec={editorSpec || null}
                operators={operators}
                notify={notify}
                onCancel={() => setMode('list')}
                onSaved={async (savedName) => {
                    setEditorSpec(undefined)
                    onSelect(savedName)
                    await refreshPipelines()
                    setMode('list')
                }}
            />
        )
    }

    // ── 模板详情 ──
    if (mode === 'detail') {
        return (
            <div className="zetl-panel">
                <h2>模板详情 <span className="zetl-badge">{name || '未选择'}</span> <button className="zetl-act" onClick={() => setMode('list')}>← 返回列表</button></h2>
                {toastEl}
                <div className="zetl-row">
                    <button className="zetl-act zetl-primary" onClick={async () => {
                        if (!name) { notify('先选一个模板', false); return }
                        try {
                            const s = spec && spec.name === name ? spec : await apiGet('/etl/pipelines/' + encodeURIComponent(name))
                            setSpec(s)
                            setEditorSpec(s)
                            setMode('editor')
                        } catch (e) { notify(errMsg(e), false) }
                    }}>画布编辑</button>
                    <button className="zetl-act" onClick={showDag}>拓扑图</button>
                    <button className="zetl-act" onClick={showDef}>定义</button>
                    <button className="zetl-act" onClick={showPlan}>计划解释</button>
                    <button className="zetl-act" onClick={showRuns}>运行履历</button>
                    <button className="zetl-act" onClick={showWatermark}>水位</button>
                    <button className="zetl-act" onClick={exportSavepoint}>导出保存点</button>
                    <button className="zetl-act" onClick={showSchedPanel}>调度登记</button>
                </div>
                <div className="zetl-row">
                    <button className="zetl-act zetl-primary" onClick={() => runPipe(false)}>运行</button>
                    <button className="zetl-act" onClick={() => runPipe(true)}>试跑（不落地）</button>
                    <input type="text" value={runParams} onChange={(e) => setRunParams(e.target.value)}
                        placeholder='运行参数 JSON，如 {"bizdate":"2026-11-21"}（可空）' style={{flex: 1, minWidth: 220}}/>
                </div>
                {extra && <div><h3 className="zetl-sec">{extra.title}</h3>{extra.node}</div>}
                {body
                    ? <div><h3 className="zetl-sec">{body.title}</h3>{body.node}</div>
                    : <p className="zetl-empty">尚未选择模板——选一个后，这里呈现拓扑 DAG、运行结果与节点指标。</p>}
            </div>
        )
    }

    // ── 模板列表 ──
    return (
        <div className="zetl-panel">
            <h2>作业模板 <button className="zetl-act zetl-primary" onClick={() => { setEditorSpec(null); setMode('editor') }}>＋ 新增模板</button></h2>
            {toastEl}
            <details className="zetl-reg">
                <summary>直接登记 yml / JSON 原文（高级）</summary>
                <div className="zetl-reg-body">
                    <textarea id="zetl-spec" spellCheck="false" defaultValue={DEMO_TEXT}/>
                    <div className="zetl-row" style={{marginTop: 10}}>
                        <button className="zetl-act zetl-primary" onClick={registerSpec}>登记</button>
                    </div>
                </div>
            </details>
            {names.length > 0 ? (
                <table>
                    <thead>
                    <tr><th>模板名</th><th style={{width: 330}}>操作</th></tr>
                    </thead>
                    <tbody>
                    {names.map((n) => (
                        <tr key={n}>
                            <td><code>{n}</code></td>
                            <td>
                                <button className="zetl-act" onClick={async () => {
                                    try {
                                        const s = await apiGet('/etl/pipelines/' + encodeURIComponent(n))
                                        setSpec(s)
                                        setEditorSpec(s)
                                        setMode('editor')
                                    } catch (e) { notify(errMsg(e), false) }
                                }}>画布编辑</button>{' '}
                                <button className="zetl-act" onClick={() => openDetail(n)}>详情</button>{' '}
                                <button className="zetl-act" onClick={() => { onSelect(n); setTimeout(() => runPipeFromList(n, false), 0) }}>运行</button>{' '}
                                <button className="zetl-act" onClick={() => { onSelect(n); setTimeout(() => runPipeFromList(n, true), 0) }}>试跑</button>
                            </td>
                        </tr>
                    ))}
                    </tbody>
                </table>
            ) : (
                <p className="zetl-empty">还没有模板——点右上「＋ 新增模板」在画布上拖出第一条流水线。</p>
            )}
        </div>
    )

    // 快捷运行：切详情并立刻触发（口径 = 控制台 quickRun）
    async function runPipeFromList(n, dry) {
        setSpec(null); setMetrics(null); setBody(null); setExtra(null); setRunParams('')
        setMode('detail')
        const path = '/etl/pipelines/' + encodeURIComponent(n) + (dry ? '/dry-run?rows=100' : '/run')
        try {
            const r = await apiPost(path)
            setMetrics(r.nodeMetrics || null)
            setBody({title: dry ? '试跑结果（不落地）' : '运行结果', node: <RunResult result={r}/>})
        } catch (e) {
            notify(errMsg(e), false)
        }
    }
}
