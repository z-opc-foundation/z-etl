/**
 * z-etl 组件包入口。
 *
 * 宿主（z-opc 主壳）用法：
 *   import '@yuku123/z-etl-component/style.css'
 *   import {configureEtl, EtlApp, TemplatesView, ...} from '@yuku123/z-etl-component'
 *   configureEtl({apiBase: '/etl/api'})   // /etl/ 前缀由壳侧 vite/nginx 剥前缀反代
 */
import './style.css'

export {default as EtlApp} from './components/EtlApp.jsx'
export {default as TemplatesView} from './components/TemplatesView.jsx'
export {default as SchedulesView} from './components/SchedulesView.jsx'
export {default as JobsView} from './components/JobsView.jsx'
export {default as DataSourcesView} from './components/DataSourcesView.jsx'
export {default as OverviewView} from './components/OverviewView.jsx'
export {SourcesView, SinksView} from './components/CatalogView.jsx'
export {default as PipelineCanvas} from './components/PipelineCanvas.jsx'
export {default as RunResult} from './components/RunResult.jsx'
export {useToast} from './components/useToast.jsx'
export {configureEtl, errMsg} from './services/api.js'
export {KIND_TAG, KIND_COLOR, KIND_STYLE, kindOf, CONFIG_KEYS, EXAMPLES} from './lib/operators.js'
