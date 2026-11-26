import {useEffect, useState} from 'react'
import {apiGet, errMsg} from '../services/api.js'
import {CONFIG_KEYS, EXAMPLES, KIND_COLOR} from '../lib/operators.js'
import {useToast} from './useToast.jsx'

function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text)
        return true
    }
    return false
}

/**
 * 算子目录（kind = SOURCE 源算子 / SINK 汇算子）。
 * props: {kind, title}
 */
export default function CatalogView({kind, title}) {
    const [rows, setRows] = useState(null) // null = 加载中
    const [err, setErr] = useState(null)
    const {notify, toastEl} = useToast()
    useEffect(() => {
        let alive = true
        apiGet('/etl/operators').then((data) => {
            if (alive) setRows((data.operators || []).filter((o) => o.kind === kind))
        }).catch((e) => { if (alive) setErr(errMsg(e)) })
        return () => { alive = false }
    }, [kind])
    return (
        <div className="zetl-root zetl-panel">
            <h2>{title}</h2>
            {toastEl}
            {err && <p style={{color: 'var(--zetl-err)'}}>目录加载失败：{err}</p>}
            {!err && !rows && <p className="zetl-empty">加载中…</p>}
            {rows && (
                <table>
                    <thead>
                    <tr><th style={{width: 170}}>类型串</th><th>说明</th><th style={{width: 430}}>配置</th></tr>
                    </thead>
                    <tbody>
                    {rows.map((o) => {
                        const ex = EXAMPLES[o.id]
                        return (
                            <tr key={o.id}>
                                <td><code>{o.id}</code></td>
                                <td>{o.desc || ''}</td>
                                <td>
                                    <details>
                                        <summary style={{cursor: 'pointer'}}>配置键 + 示例</summary>
                                        <p style={{margin: '6px 0 4px', color: 'var(--zetl-dim)'}}>
                                            <code>{CONFIG_KEYS[o.id] || '—'}</code>
                                        </p>
                                        {ex
                                            ? (
                                                <>
                                                    <pre>{JSON.stringify(ex, null, 2)}</pre>
                                                    <div className="zetl-row">
                                                        <button className="zetl-act" style={{borderLeft: '3px solid ' + KIND_COLOR[o.kind]}}
                                                            onClick={() => {
                                                                if (copyText(JSON.stringify(ex, null, 2))) notify('已复制 ' + o.id + ' 示例', true)
                                                                else notify('浏览器不支持剪贴板 API，请手动复制', false)
                                                            }}>复制示例</button>
                                                    </div>
                                                </>
                                            )
                                            : <p style={{color: 'var(--zetl-dim)'}}>暂无示例</p>}
                                    </details>
                                </td>
                            </tr>
                        )
                    })}
                    </tbody>
                </table>
            )}
        </div>
    )
}

export function SourcesView() {
    return <CatalogView kind="SOURCE" title="数据源插件目录（源算子，开箱即用）"/>
}

export function SinksView() {
    return <CatalogView kind="SINK" title="输出源插件目录（汇算子，开箱即用）"/>
}
