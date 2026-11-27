import {useEffect, useState} from 'react'
import {apiGet, errMsg} from '../services/api.js'
import {fmtTime} from '../lib/dag.js'
import {useToast} from './useToast.jsx'

function StatusChip({status}) {
    if (!status) return <span className="zetl-badge">未运行</span>
    const failed = status === 'FAILED'
    return (
        <span className="zetl-badge" style={failed
            ? {color: 'var(--zetl-err)', borderColor: 'var(--zetl-err)', background: 'rgba(255,77,79,.08)'}
            : {color: 'var(--zetl-ok)', borderColor: 'var(--zetl-ok)', background: 'rgba(82,196,26,.08)'}}>
            {failed ? '失败' : '成功'}
        </span>
    )
}

function StatCard({label, value, tone}) {
    return (
        <div className="zetl-panel zetl-stat">
            <div className="zetl-stat-label">{label}</div>
            <div className="zetl-stat-value" style={tone ? {color: tone} : null}>{value}</div>
        </div>
    )
}

/**
 * 运行总览：窗口计数 + 每条流水线最近一次运行 + 最近运行明细。
 * 数据面：GET /etl/pipelines/overview。
 */
export default function OverviewView() {
    const [data, setData] = useState(null)
    const [err, setErr] = useState(null)
    const {notify, toastEl} = useToast()

    useEffect(() => {
        let alive = true
        apiGet('/etl/pipelines/overview?recent=50').then((d) => {
            if (alive) setData(d)
        }).catch((e) => { if (alive) setErr(errMsg(e)) })
        return () => { alive = false }
    }, [])

    const fmtMs = (ms) => (ms == null || ms < 0 ? '—' : (ms >= 1000 ? (ms / 1000).toFixed(1) + 's' : ms + 'ms'))

    const pipelines = data ? data.pipelines || [] : []
    const recent = data ? data.recentRuns || [] : []
    const h24 = data ? data.h24 || {} : {}
    const d7 = data ? data.d7 || {} : {}
    const failedIn24 = h24.failed || 0

    return (
        <div className="zetl-root">
            <div className="zetl-row" style={{alignItems: 'stretch', gap: 10, marginBottom: 4}}>
                <StatCard label="流水线总数" value={pipelines.length}/>
                <StatCard label="近 24h 成功" value={h24.success ?? 0} tone="var(--zetl-ok)"/>
                <StatCard label="近 24h 失败" value={failedIn24} tone={failedIn24 > 0 ? 'var(--zetl-err)' : null}/>
                <StatCard label="近 7d 成功 / 失败" value={(d7.success ?? 0) + ' / ' + (d7.failed ?? 0)}/>
            </div>
            {toastEl}
            {err && <div className="zetl-panel"><p style={{color: 'var(--zetl-err)'}}>总览加载失败：{err}</p></div>}
            {!err && !data && <div className="zetl-panel"><p className="zetl-empty">加载中…</p></div>}
            {data && (
                <>
                    <div className="zetl-panel">
                        <h2>流水线健康 <span className="zetl-badge">最近一次运行</span></h2>
                        {pipelines.length === 0
                            ? <p className="zetl-empty">还没有流水线——先到「作业模板」登记一条。</p>
                            : (
                                <table>
                                    <thead>
                                    <tr>
                                        <th>流水线</th><th>最近状态</th><th>上次开始</th><th>耗时</th>
                                        <th>读 / 写</th><th>调度</th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    {pipelines.map((p) => (
                                        <tr key={p.name}>
                                            <td><code>{p.name}</code></td>
                                            <td><StatusChip status={p.lastStatus}/></td>
                                            <td>{p.lastStartedAt ? fmtTime(p.lastStartedAt) : '—'}</td>
                                            <td>{fmtMs(p.lastDurationMs)}</td>
                                            <td>{p.lastRecordsRead ?? '—'} / {p.lastRecordsWritten ?? '—'}</td>
                                            <td>
                                                {p.schedule
                                                    ? <span>{p.schedule.cronExpression}
                                                        {p.schedule.enabled === false && <span className="zetl-badge" style={{marginLeft: 6}}>停用</span>}
                                                        {p.schedule.nextFireTime
                                                            && <span style={{color: 'var(--zetl-dim)', marginLeft: 6}}>下次 {fmtTime(p.schedule.nextFireTime)}</span>}
                                                    </span>
                                                    : <span style={{color: 'var(--zetl-dim)'}}>未登记</span>}
                                            </td>
                                        </tr>
                                    ))}
                                    </tbody>
                                </table>
                            )}
                    </div>
                    <div className="zetl-panel">
                        <h2>最近运行 <span className="zetl-badge">跨流水线 · 含失败</span></h2>
                        {recent.length === 0
                            ? <p className="zetl-empty">还没有运行记录。</p>
                            : (
                                <table>
                                    <thead>
                                    <tr>
                                        <th>时间</th><th>流水线</th><th>状态</th><th>模式</th><th>耗时</th><th>读 / 写</th><th>备注</th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    {recent.map((r, i) => (
                                        <tr key={i}>
                                            <td>{fmtTime(r.startedAt)}</td>
                                            <td><code>{r.pipeline}</code></td>
                                            <td><StatusChip status={r.status}/></td>
                                            <td>{r.mode || '—'}</td>
                                            <td>{fmtMs(r.durationMs)}</td>
                                            <td>{r.recordsRead ?? '—'} / {r.recordsWritten ?? '—'}</td>
                                            <td style={{color: r.status === 'FAILED' ? 'var(--zetl-err)' : 'var(--zetl-dim)', maxWidth: 420}}>
                                                {(r.notes || []).join('；') || '—'}
                                            </td>
                                        </tr>
                                    ))}
                                    </tbody>
                                </table>
                            )}
                    </div>
                </>
            )}
        </div>
    )
}
