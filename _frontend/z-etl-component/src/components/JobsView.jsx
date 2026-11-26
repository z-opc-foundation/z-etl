import {useEffect, useState} from 'react'
import {apiGet, apiGetMaybe404, apiPost, errMsg} from '../services/api.js'
import {fmtTime} from '../lib/dag.js'
import RunResult, {JobRunsTable} from './RunResult.jsx'
import {useToast} from './useToast.jsx'

/** 作业实例：登记（yml/JSON 原文）+ 详情（描述 / 运行 / 履历 / 水位）。 */
export default function JobsView({selected, onSelect, notify: notifyProp, toastEl: toastProp, onCount}) {
    const {notify: notifySelf, toastEl: toastSelf} = useToast()
    const notify = notifyProp || notifySelf
    const toastEl = toastProp || toastSelf
    const [jobs, setJobs] = useState([])
    const [jobdesc, setJobdesc] = useState('')
    const [body, setBody] = useState(null) // {title, node}
    const name = selected || null

    useEffect(() => {
        apiGet('/etl/jobs').then((data) => {
            const names = data.jobs || []
            setJobs(names)
            if (onCount) onCount(names.length)
        }).catch((e) => notify('作业列表加载失败：' + errMsg(e), false))
    }, [])

    const needJob = () => {
        if (!name) { notify('先选一个作业', false); return false }
        return true
    }

    const showJob = async (jobName) => {
        const target = jobName || name
        if (!target) { notify('先选一个作业', false); return }
        try {
            const desc = await apiGet('/etl/jobs/' + encodeURIComponent(target))
            setBody({title: '作业描述', node: <pre>{JSON.stringify(desc, null, 2)}</pre>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const runJob = async () => {
        if (!needJob()) return
        setBody({title: '运行中…', node: null})
        try {
            const r = await apiPost('/etl/jobs/' + encodeURIComponent(name) + '/run')
            setBody({title: '运行结果', node: <RunResult result={r}/>})
        } catch (e) { notify(errMsg(e), false); setBody(null) }
    }

    const showJobRuns = async () => {
        if (!needJob()) return
        try {
            const data = await apiGet('/etl/jobs/' + encodeURIComponent(name) + '/runs?limit=20')
            setBody({title: '运行履历', node: <JobRunsTable runs={data.runs || []}/>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const showJobWatermark = async () => {
        if (!needJob()) return
        try {
            const wm = await apiGetMaybe404('/etl/jobs/' + encodeURIComponent(name) + '/watermark')
            setBody({title: '水位记录', node: wm
                ? <pre>{JSON.stringify(wm, null, 2)}</pre>
                : <p style={{color: 'var(--zetl-dim)'}}>还没有水位</p>})
        } catch (e) { notify(errMsg(e), false) }
    }

    const registerJob = async () => {
        try {
            const data = await apiPost('/etl/jobs', jobdesc, {raw: true})
            notify('已登记：' + data.name, true)
            onSelect(data.name)
            setJobs((prev) => (prev.includes(data.name) ? prev : [...prev, data.name]))
            showJob(data.name)
        } catch (e) { notify(errMsg(e), false) }
    }

    return (
        <div className="zetl-root">
            <details className="zetl-reg">
                <summary>登记作业（yml / JSON 原文）</summary>
                <div className="zetl-reg-body">
                    <textarea spellCheck="false" value={jobdesc} onChange={(e) => setJobdesc(e.target.value)}/>
                    <div className="zetl-row" style={{marginTop: 10}}>
                        <button className="zetl-act zetl-primary" onClick={registerJob}>登记</button>
                    </div>
                </div>
            </details>
            <div className="zetl-panel">
                <h2>作业详情 <span className="zetl-badge">{name || '未选择'}</span></h2>
                {toastEl}
                <div className="zetl-row">
                    <button className="zetl-act" onClick={showJob}>作业描述</button>
                    <button className="zetl-act zetl-primary" onClick={runJob}>运行（首跑全量，之后增量）</button>
                    <button className="zetl-act" onClick={showJobRuns}>运行履历</button>
                    <button className="zetl-act" onClick={showJobWatermark}>水位</button>
                </div>
                {jobs.length > 0 && (
                    <div className="zetl-row">
                        {jobs.map((n) => (
                            <button key={n} className={'zetl-act' + (n === name ? ' zetl-on' : '')}
                                title={n} onClick={() => { onSelect(n) }}>{n}</button>
                        ))}
                    </div>
                )}
                {body
                    ? <div><h3 className="zetl-sec">{body.title}</h3>{body.node}</div>
                    : <p className="zetl-empty">尚未选择作业——上方点选一个后，这里呈现运行结果与履历。</p>}
            </div>
        </div>
    )
}
