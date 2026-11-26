import {useEffect, useState} from 'react'
import {apiGet, errMsg} from '../services/api.js'
import {fmtTime} from '../lib/dag.js'

export default function SchedulesView({notify, toastEl, onCount}) {
    const [list, setList] = useState(null)
    const [err, setErr] = useState(null)
    useEffect(() => {
        let alive = true
        apiGet('/etl/pipelines/schedules').then((data) => {
            if (!alive) return
            const schedules = data.schedules || []
            setList(schedules)
            if (onCount) onCount(schedules.length)
        }).catch((e) => { if (alive) setErr(errMsg(e)) })
        return () => { alive = false }
    }, [])
    return (
        <div className="zetl-panel">
            <h2>全部调度登记</h2>
            {toastEl}
            {err && <p style={{color: 'var(--zetl-err)'}}>{err}</p>}
            {!err && list == null && <p className="zetl-empty">加载中…</p>}
            {list && list.length > 0 && (
                <table>
                    <thead>
                    <tr><th>流水线</th><th>cron</th><th>启用</th><th>上次触发</th><th>下次触发</th></tr>
                    </thead>
                    <tbody>
                    {list.map((s, i) => (
                        <tr key={i}>
                            <td>{s.pipelineName}</td>
                            <td><code>{s.cronExpression}</code></td>
                            <td>{s.enabled ? '是' : '否'}</td>
                            <td>{fmtTime(s.lastTriggeredAt)}</td>
                            <td>{fmtTime(s.nextFireTime)}</td>
                        </tr>
                    ))}
                    </tbody>
                </table>
            )}
            {list && list.length === 0 && <p style={{color: 'var(--zetl-dim)'}}>没有调度登记</p>}
        </div>
    )
}
