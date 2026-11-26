import {fmtTime} from '../lib/dag.js'

function kvTable(obj) {
    const keys = Object.keys(obj || {})
    return (
        <table>
            <tbody>
            {keys.map((k) => (
                <tr key={k}>
                    <th>{k}</th>
                    <td>{typeof obj[k] === 'object' && obj[k] !== null ? JSON.stringify(obj[k]) : String(obj[k])}</td>
                </tr>
            ))}
            </tbody>
        </table>
    )
}

/** 运行结果 + 节点指标（流水线与作业共用口径）。 */
export default function RunResult({result}) {
    if (!result || typeof result !== 'object') {
        return <pre>{typeof result === 'string' ? result : JSON.stringify(result, null, 2)}</pre>
    }
    const metrics = result.nodeMetrics || {}
    return (
        <div>
            <div className="zetl-kv">
                <b>读 / 写</b>
                <span>{result.recordsRead} / {result.recordsWritten}</span>
                <b>模式</b>
                <span>{result.mode || '—'}{result.restored ? '（恢复自状态快照）' : ''}</span>
                <b>水位</b>
                <span>{result.watermarkBefore == null ? '—' : String(result.watermarkBefore)} → {result.watermarkAfter == null ? '—' : String(result.watermarkAfter)}</span>
            </div>
            {result.notes && result.notes.length > 0 && (
                <p style={{color: 'var(--zetl-warn)', fontSize: 12}}>{result.notes.join('；')}</p>
            )}
            <h3 className="zetl-sec">节点指标</h3>
            <table>
                <thead>
                <tr><th>节点</th><th>records-in</th><th>records-out</th><th>其余计数</th></tr>
                </thead>
                <tbody>
                {Object.keys(metrics).map((n) => {
                    const m = metrics[n] || {}
                    const rest = Object.keys(m)
                        .filter((k) => k !== 'records-in' && k !== 'records-out' && k !== 'elapsed-nanos')
                        .map((k) => k + '=' + m[k]).join('，')
                    return (
                        <tr key={n}>
                            <td>{n}</td>
                            <td>{m['records-in'] == null ? '—' : String(m['records-in'])}</td>
                            <td>{m['records-out'] == null ? '—' : String(m['records-out'])}</td>
                            <td>{rest || '—'}</td>
                        </tr>
                    )
                })}
                </tbody>
            </table>
        </div>
    )
}

/** 作业运行履历表（jobs 的 runs 字段口径：rowsRead/rowsWritten/reconciled）。 */
export function JobRunsTable({runs}) {
    if (!runs || !runs.length) return <p style={{color: 'var(--zetl-dim)'}}>还没有运行记录</p>
    return (
        <table>
            <thead>
            <tr><th>时间</th><th>模式</th><th>读</th><th>写</th><th>对账</th><th>水位</th></tr>
            </thead>
            <tbody>
            {runs.map((r, i) => (
                <tr key={i}>
                    <td>{fmtTime(r.startedAt)}</td>
                    <td>{r.mode || ''}</td>
                    <td>{String(r.rowsRead)}</td>
                    <td>{String(r.rowsWritten)}</td>
                    <td>{r.reconciled ? '是' : '否'}</td>
                    <td>{r.watermarkAfter || '—'}</td>
                </tr>
            ))}
            </tbody>
        </table>
    )
}
