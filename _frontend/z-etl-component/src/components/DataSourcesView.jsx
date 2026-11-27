import {useEffect, useState} from 'react'
import {apiGet, apiPost, apiDelete, errMsg} from '../services/api.js'
import {useToast} from './useToast.jsx'

const EMPTY_FORM = {name: '', type: 'jdbc', url: '', username: '', password: '', description: ''}

/**
 * 数据源管理：登记 / 编辑 / 删除 / 测试连接。
 * 数据面：/etl/datasources（列表密码掩码不出网；编辑留空密码 = 保留旧密码）。
 * 流水线声明里的 dataSourceRef 按名指向这里的 name。
 */
export default function DataSourcesView({notify: notifyProp, toastEl: toastProp, onCount}) {
    const {notify: notifySelf, toastEl: toastSelf} = useToast()
    const notify = notifyProp || notifySelf
    const toastEl = toastProp || toastSelf
    const [rows, setRows] = useState(null) // null = 加载中
    const [form, setForm] = useState(EMPTY_FORM)
    const [editing, setEditing] = useState(false)
    const [testing, setTesting] = useState(null) // 正在测试的名字

    const load = () => {
        apiGet('/etl/datasources').then((data) => {
            const list = data.datasources || []
            setRows(list)
            if (onCount) onCount(list.length)
        }).catch((e) => { setRows([]); notify('数据源列表加载失败：' + errMsg(e), false) })
    }
    useEffect(load, [])

    const setField = (key) => (e) => setForm((f) => ({...f, [key]: e.target.value}))

    const startCreate = () => {
        setEditing(false)
        setForm(EMPTY_FORM)
    }
    const startEdit = (row) => {
        setEditing(true)
        setForm({
            name: row.name, type: row.type || 'jdbc', url: row.url || '',
            username: row.username || '', password: '', // 掩码不出网，留空 = 保留旧密码
            description: row.description || '',
        })
    }

    const save = async () => {
        if (!form.name.trim()) { notify('数据源名不能为空', false); return }
        if (!form.url.trim()) { notify('JDBC url 不能为空', false); return }
        try {
            await apiPost('/etl/datasources', form)
            notify((editing ? '已更新：' : '已登记：') + form.name, true)
            setEditing(false)
            setForm(EMPTY_FORM)
            load()
        } catch (e) { notify(errMsg(e), false) }
    }

    const testForm = async () => {
        if (!form.name.trim() || !form.url.trim()) { notify('先填数据源名和 url', false); return }
        setTesting(form.name)
        try {
            const r = await apiPost('/etl/datasources/test', form)
            notify(r.message, r.ok === true)
        } catch (e) { notify(errMsg(e), false) } finally { setTesting(null) }
    }

    const testRow = async (row) => {
        setTesting(row.name)
        try {
            // 行级测试：password 不回传（留空），后端对已登记条目用库存真值
            const r = await apiPost('/etl/datasources/test', {name: row.name, url: row.url, username: row.username, password: ''})
            notify(row.name + '：' + r.message, r.ok === true)
        } catch (e) { notify(errMsg(e), false) } finally { setTesting(null) }
    }

    const remove = async (row) => {
        if (!window.confirm('删除数据源 ' + row.name + '？引用它的流水线运行时会报「数据源未登记」。')) return
        try {
            await apiDelete('/etl/datasources/' + encodeURIComponent(row.name))
            notify('已删除：' + row.name, true)
            load()
        } catch (e) { notify(errMsg(e), false) }
    }

    const field = (label, key, props) => (
        <label className="zetl-field">
            <span>{label}</span>
            <input value={form[key]} onChange={setField(key)} {...props}/>
        </label>
    )

    return (
        <div className="zetl-root">
            <div className="zetl-panel">
                <h2>{editing ? '编辑数据源' : '登记数据源'}
                    <span className="zetl-badge">dataSourceRef 按名引用</span>
                    {editing && <button className="zetl-act" style={{marginLeft: 'auto'}} onClick={startCreate}>取消编辑，改新增</button>}
                </h2>
                {toastEl}
                <div className="zetl-form-grid">
                    {field('名称', 'name', {placeholder: 'ds-oc-main（流水线里 dataSourceRef 用这个名字）', disabled: editing})}
                    {field('类型', 'type', {placeholder: 'jdbc'})}
                    {field('JDBC url', 'url', {placeholder: 'jdbc:mysql://host:3306/db', style: {gridColumn: '1 / -1'}})}
                    {field('用户名', 'username', {placeholder: '账号'})}
                    {field('密码', 'password', {placeholder: editing ? '留空 = 保留旧密码' : '密码（只落库，接口不出网）', type: 'password'})}
                    {field('备注', 'description', {placeholder: '用途说明', style: {gridColumn: '1 / -1'}})}
                </div>
                <div className="zetl-row">
                    <button className="zetl-act zetl-primary" onClick={save}>{editing ? '保存修改' : '登记'}</button>
                    <button className="zetl-act" onClick={testForm} disabled={testing != null}>
                        {testing === form.name ? '测试中…' : '测试连接'}
                    </button>
                </div>
            </div>
            <div className="zetl-panel">
                <h2>已登记数据源 <span className="zetl-badge">{rows ? rows.length : '…'}</span></h2>
                {!rows && <p className="zetl-empty">加载中…</p>}
                {rows && rows.length === 0 && <p className="zetl-empty">还没有登记数据源——上方填好后点「登记」。</p>}
                {rows && rows.length > 0 && (
                    <table>
                        <thead>
                        <tr><th style={{width: 150}}>名称</th><th style={{width: 70}}>类型</th><th>url</th>
                            <th style={{width: 110}}>用户名</th><th style={{width: 90}}>密码</th><th>备注</th><th style={{width: 170}}>操作</th></tr>
                        </thead>
                        <tbody>
                        {rows.map((row) => (
                            <tr key={row.name}>
                                <td><code>{row.name}</code></td>
                                <td>{row.type}</td>
                                <td style={{maxWidth: 380, overflow: 'hidden', textOverflow: 'ellipsis'}} title={row.url}>{row.url}</td>
                                <td>{row.username || '—'}</td>
                                <td>{row.password ? <span className="zetl-badge">已存</span> : <span style={{color: 'var(--zetl-dim)'}}>未存</span>}</td>
                                <td style={{color: 'var(--zetl-dim)'}}>{row.description || '—'}</td>
                                <td>
                                    <div className="zetl-row" style={{gap: 6}}>
                                        <button className="zetl-act" onClick={() => testRow(row)} disabled={testing != null}>
                                            {testing === row.name ? '测试中…' : '测试连接'}
                                        </button>
                                        <button className="zetl-act" onClick={() => startEdit(row)}>编辑</button>
                                        <button className="zetl-act" style={{color: 'var(--zetl-err)'}} onClick={() => remove(row)}>删除</button>
                                    </div>
                                </td>
                            </tr>
                        ))}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    )
}
