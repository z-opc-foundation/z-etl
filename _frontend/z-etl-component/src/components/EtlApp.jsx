/**
 * z-etl 工作台（独立形态：左树 + 七视图 + hash 路由）。
 *
 * 嵌主壳时不要用这个组件——按视图单独挂 DataSourcesView / SourcesView / SinksView /
 * TemplatesView / SchedulesView / JobsView / OverviewView（壳的菜单已承担左树职责）；
 * EtlApp 供独立页面 / suit 冒烟用。
 *
 * props:
 *   view        受控视图键（datasources/overview/templates/job/sched/sources/sinks，复数别名归一）；
 *               缺省 = 内部状态 + location.hash 路由（独立形态）
 *   defaultView 初始视图（默认 overview）
 */
import {useEffect, useMemo, useState} from 'react'
import {apiGet} from '../services/api.js'
import {useToast} from './useToast.jsx'
import {SourcesView, SinksView} from './CatalogView.jsx'
import DataSourcesView from './DataSourcesView.jsx'
import OverviewView from './OverviewView.jsx'
import TemplatesView from './TemplatesView.jsx'
import SchedulesView from './SchedulesView.jsx'
import JobsView from './JobsView.jsx'

const TABS = ['datasources', 'overview', 'sources', 'sinks', 'templates', 'sched', 'job']
const TAB_ALIAS = {schedules: 'sched', jobs: 'job', datasource: 'datasources'}
const normTab = (t) => TAB_ALIAS[t] || t

const ICONS = {
    brand: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 2 2 7l10 5 10-5-10-5Z"/><path d="m2 17 10 5 10-5"/><path d="m2 12 10 5 10-5"/></svg>
    ),
    chevron: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
    ),
    templates: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="8" height="8" rx="2"/><path d="M7 11v4a2 2 0 0 0 2 2h4"/><rect x="13" y="13" width="8" height="8" rx="2"/></svg>
    ),
    job: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
    ),
    sched: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
    ),
    sources: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/></svg>
    ),
    sinks: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
    ),
    datasources: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
    ),
    overview: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 3v18h18"/><path d="m19 9-5 5-4-4-3 3"/></svg>
    ),
    collapse: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="17 11 12 6 7 11"/><polyline points="17 18 12 13 7 18"/></svg>
    ),
}

const NAV_GROUPS_KEY = 'etl.navCollapsedGroups'

function readCollapsedGroups() {
    try {
        const parsed = JSON.parse(localStorage.getItem(NAV_GROUPS_KEY) || '{}')
        return parsed && typeof parsed === 'object' ? parsed : {}
    } catch (e) { return {} }
}

export default function EtlApp({view, defaultView, style}) {
    const controlled = view !== undefined
    const [innerView, setInnerView] = useState(normTab(defaultView || 'templates'))
    const current = normTab(controlled ? view : innerView)
    const [operators, setOperators] = useState([])
    const [counts, setCounts] = useState({templates: 0, job: 0, sched: 0, sources: 0, sinks: 0, datasources: 0})
    const [pipeline, setPipeline] = useState(null)
    const [job, setJob] = useState(null)
    const [collapsed, setCollapsed] = useState(readCollapsedGroups)
    const {notify, toastEl} = useToast()

    const setTab = (tab, skipHash) => {
        const t = normTab(tab)
        if (!controlled) {
            setInnerView(t)
            if (!skipHash && typeof history !== 'undefined') history.replaceState(null, '', '#' + t)
        }
    }

    useEffect(() => {
        apiGet('/etl/operators').then((data) => {
            const ops = data.operators || []
            setOperators(ops)
            setCounts((c) => ({...c, sources: ops.filter((o) => o.kind === 'SOURCE').length, sinks: ops.filter((o) => o.kind === 'SINK').length}))
        }).catch(() => {})
    }, [])

    useEffect(() => {
        if (controlled) return undefined
        const onHash = () => {
            const t = normTab(location.hash.replace('#', ''))
            if (TABS.indexOf(t) >= 0) setInnerView(t)
        }
        window.addEventListener('hashchange', onHash)
        const initial = normTab(location.hash.replace('#', ''))
        if (TABS.indexOf(initial) >= 0) setInnerView(initial)
        return () => window.removeEventListener('hashchange', onHash)
    }, [controlled])

    const toggleGroup = (key) => {
        setCollapsed((prev) => {
            const next = {...prev, [key]: !prev[key]}
            try { localStorage.setItem(NAV_GROUPS_KEY, JSON.stringify(next)) } catch (e) { /* 隐私模式本次生效 */ }
            return next
        })
    }

    const collapseAll = () => {
        setCollapsed((prev) => {
            const anyOpen = Object.values(prev).some((v) => !v)
            const next = {}
            Object.keys(prev).forEach((k) => { next[k] = anyOpen })
            try { localStorage.setItem(NAV_GROUPS_KEY, JSON.stringify(next)) } catch (e) { /* 本次生效 */ }
            return next
        })
    }

    const itemCount = (n) => (n > 0 ? String(n) : '')

    const views = useMemo(() => ({
        datasources: (
            <DataSourcesView
                notify={notify}
                onCount={(n) => setCounts((c) => ({...c, datasources: n}))}
            />
        ),
        overview: <OverviewView/>,
        sources: <SourcesView/>,
        sinks: <SinksView/>,
        templates: (
            <TemplatesView
                selected={pipeline}
                onSelect={setPipeline}
                notify={notify}
                operators={operators}
                onCount={(n) => setCounts((c) => ({...c, templates: n}))}
            />
        ),
        sched: (
            <SchedulesView
                notify={notify}
                onCount={(n) => setCounts((c) => ({...c, sched: n}))}
            />
        ),
        job: (
            <JobsView
                selected={job}
                onSelect={setJob}
                notify={notify}
                onCount={(n) => setCounts((c) => ({...c, job: n}))}
            />
        ),
    }), [current, pipeline, job, operators, toastEl])

    const navItem = (tab, label, icon, count) => (
        <div className={'zetl-nav-item' + (current === tab ? ' active' : '')} onClick={() => setTab(tab)}>
            {icon}
            <span className="zetl-nav-text">{label}</span>
            <span className="zetl-nav-badge">{itemCount(counts[count])}</span>
        </div>
    )

    const dsActive = current === 'datasources'
    const jobsMgmtActive = ['templates', 'job', 'sched'].indexOf(current) >= 0
    const opsActive = ['sources', 'sinks'].indexOf(current) >= 0

    return (
        <div className="zetl-root zetl-layout" style={style}>
            <aside className="zetl-side">
                <div className="zetl-brand-row">
                    <span className="zetl-brand-mark">{ICONS.brand}</span>
                    <div className="zetl-brand-copy">
                        <div className="zetl-brand-name">z-etl</div>
                        <div className="zetl-brand-sub">数据流水线 · 流处理口径</div>
                    </div>
                </div>
                <nav className="zetl-nav">
                    <div className={'zetl-nav-group' + (dsActive ? ' has-active' : '') + (collapsed.datasources ? ' collapsed' : '')}>
                        <button type="button" className="zetl-nav-group-toggle" onClick={(ev) => { ev.stopPropagation(); toggleGroup('datasources') }}>
                            <span>数据接入</span>
                            <span className="zetl-nav-group-active"/>
                            <span className="zetl-nav-group-chevron">{ICONS.chevron}</span>
                        </button>
                        <div className="zetl-nav-items">
                            {navItem('datasources', '数据源管理', ICONS.datasources, 'datasources')}
                        </div>
                    </div>
                    <div className={'zetl-nav-group' + (jobsMgmtActive ? ' has-active' : '') + (collapsed['jobs-mgmt'] ? ' collapsed' : '')}>
                        <button type="button" className="zetl-nav-group-toggle" onClick={(ev) => { ev.stopPropagation(); toggleGroup('jobs-mgmt') }}>
                            <span>作业管理</span>
                            <span className="zetl-nav-group-active"/>
                            <span className="zetl-nav-group-chevron">{ICONS.chevron}</span>
                        </button>
                        <div className="zetl-nav-items">
                            {navItem('templates', '作业模板', ICONS.templates, 'templates')}
                            <ul className="zetl-nav-subitems">
                                {(pipeline ? [pipeline] : []).map((n) => (
                                    <li key={n} className="on" title={n} onClick={() => setTab('templates', true)}>{n}</li>
                                ))}
                                {!pipeline && <li className="none">（未选择模板）</li>}
                            </ul>
                            {navItem('job', '作业实例', ICONS.job, 'job')}
                            <ul className="zetl-nav-subitems">
                                {(job ? [job] : []).map((n) => (
                                    <li key={n} className="on" title={n} onClick={() => setTab('job', true)}>{n}</li>
                                ))}
                                {!job && <li className="none">（未选择作业）</li>}
                            </ul>
                            {navItem('sched', '调度', ICONS.sched, 'sched')}
                        </div>
                    </div>
                    <div className={'zetl-nav-group' + (current === 'overview' ? ' has-active' : '') + (collapsed.ops ? ' collapsed' : '')}>
                        <button type="button" className="zetl-nav-group-toggle" onClick={(ev) => { ev.stopPropagation(); toggleGroup('ops') }}>
                            <span>运维治理</span>
                            <span className="zetl-nav-group-active"/>
                            <span className="zetl-nav-group-chevron">{ICONS.chevron}</span>
                        </button>
                        <div className="zetl-nav-items">
                            {navItem('overview', '运行总览', ICONS.overview)}
                        </div>
                    </div>
                    <div className={'zetl-nav-group' + (opsActive ? ' has-active' : '') + (collapsed['ops-catalog'] ? ' collapsed' : '')}>
                        <button type="button" className="zetl-nav-group-toggle" onClick={(ev) => { ev.stopPropagation(); toggleGroup('ops-catalog') }}>
                            <span>算子库</span>
                            <span className="zetl-nav-group-active"/>
                            <span className="zetl-nav-group-chevron">{ICONS.chevron}</span>
                        </button>
                        <div className="zetl-nav-items">
                            {navItem('sources', '源算子', ICONS.sources, 'sources')}
                            {navItem('sinks', '汇算子', ICONS.sinks, 'sinks')}
                        </div>
                    </div>
                </nav>
                <div className="zetl-sidebar-tools">
                    <button type="button" className="zetl-sidebar-tool-btn" onClick={collapseAll}>
                        {ICONS.collapse}
                        <span>收起全部</span>
                    </button>
                </div>
            </aside>
            <main className="zetl-main">
                {views[current] || views.templates}
                {toastEl}
            </main>
        </div>
    )
}
