/**
 * 算子目录常量与示例（真源 = 控制台 static/index.html 同步口径，2026-11-26 组件化照搬）。
 * 服务端只给 {id, kind, desc}，配置键与示例是前端知识，跟算子波次同步维护。
 */
export const KIND_TAG = {SOURCE: '源', TRANSFORM: '变换', STATEFUL: '有状态', SINK: '汇'}
export const KIND_COLOR = {SOURCE: '#389e0d', TRANSFORM: '#8c8c8c', STATEFUL: '#1677ff', SINK: '#d46b08'}
export const KIND_STYLE = {
    source: {stroke: '#389e0d', tag: '源'},
    transform: {stroke: '#8c8c8c', tag: '变换'},
    stateful: {stroke: '#1677ff', tag: '有状态'},
    sink: {stroke: '#d46b08', tag: '汇'}
}

export function kindOf(type) {
    const t = String(type || '')
    if (/source|pipeline\.ref$/.test(t)) return 'source'
    if (/sink$/.test(t)) return 'sink'
    if (/^(aggregate|window\.aggregate|dedup|join|interval\.join|dim\.join|cep\.pattern)$/.test(t)) return 'stateful'
    return 'transform'
}

export const CONFIG_KEYS = {
    'jdbc.source': 'table/columns/where/orderBy/pageSize/eventTimeColumn/eventTimeType/maxOutOfOrderness/resume/pushdown/dataSourceRef',
    'file.csv.source': 'path/delimiter/quote/header/encoding/columns/columnTypes/skipRows/limit/eventTimeColumn',
    'file.json.source': 'path/format/itemPath/columns/columnTypes/encoding/eventTimeColumn',
    'file.xlsx.source': 'path/sheet/header/columns/columnTypes',
    'file.xml.source': 'path/recordPath/encoding/columns/columnTypes/limit',
    'http.source': 'url/method/headers/body/itemPath/pageParam/pageSize/maxPages/allowedHosts',
    'mill.source': 'baseUrl/file/url/mode/timeoutMs',
    'inline.source': 'rows/columnTypes',
    'script.source': 'script/engine/name/timeoutMs/maxOutputChars',
    'message.source': 'topic/pollSize',
    'pipeline.ref': 'name/params/eventTimeColumn',
    'ai.map': 'prompt/outputField/system/agentCode',
    'jdbc.sink': 'dataSourceRef/table/batchSize/writeMode/columns/idempotencyKey',
    'file.csv.sink': 'path/columns/delimiter/quote/encoding/append',
    'file.json.sink': 'path/format/columns/encoding/append',
    'file.xlsx.sink': 'path/sheetName/header/columns',
    'file.xml.sink': 'path/rootName/recordName/encoding/declaration/columns',
    'http.sink': 'url/method/headers/format/timeoutMs',
    'message.sink': 'topic/keyColumn/keyField/format',
    'console.sink': '（无配置）',
    'collect.sink': '（无配置）'
}

export const EXAMPLES = {
    'jdbc.source': {table: 'demo_src', columns: ['id', 'name', 'amount'], orderBy: 'id', pageSize: 1000, dataSourceRef: 'ds-src'},
    'file.csv.source': {path: '/data/in.csv', delimiter: ',', header: true},
    'file.json.source': {path: '/data/in.json', format: 'ndjson'},
    'file.xlsx.source': {path: '/data/in.xlsx', header: true},
    'file.xml.source': {path: '/data/in.xml', recordPath: 'row'},
    'http.source': {url: 'https://api.example.com/list', pageParam: 'page', pageSize: 100, maxPages: 10},
    'mill.source': {url: 'https://example.com/doc.html', mode: 'chunk'},
    'inline.source': {rows: [{id: 1, user: '甲', amount: 100}, {id: 2, user: '乙', amount: 200}]},
    'script.source': {script: "return [[1,'甲'],[2,'乙']]", engine: 'GROOVY'},
    'message.source': {topic: 'etl-in', pollSize: 100},
    'pipeline.ref': {name: 'another-pipeline'},
    'jdbc.sink': {table: 'demo_out', batchSize: 500, writeMode: 'upsert', idempotencyKey: ['id'], dataSourceRef: 'ds-dst'},
    'file.csv.sink': {path: '/data/out.csv'},
    'file.json.sink': {path: '/data/out.json', format: 'ndjson'},
    'file.xlsx.sink': {path: '/data/out.xlsx', sheetName: 'result'},
    'file.xml.sink': {path: '/data/out.xml', rootName: 'rows', recordName: 'row'},
    'http.sink': {url: 'https://example.com/hook', format: 'json'},
    'message.sink': {topic: 'etl-out', format: 'json'},
    'console.sink': {},
    'collect.sink': {},
    'filter': {expression: 'amount > 0', engine: 'EL', rejectedTo: 'dead'},
    'route': {rules: [{to: 'vip', expression: 'amount > 10000'}, {to: 'normal', expression: 'amount > 0'}], fallback: 'dead'},
    'map': {columns: [{name: 'amount_cny', expression: 'amount * 7.2', engine: 'EL', type: 'BIGDECIMAL'}]},
    'index': {name: 'row_num', start: 1},
    'script': {name: 'channel', engine: 'GROOVY', mode: 'column', script: "payment_method == null ? '' : payment_method.trim().toUpperCase()"},
    'script.remote': {baseUrl: 'http://z-script:8090', scriptCode: 'clean-channel', mode: 'column', name: 'channel'},
    'async.map': {url: 'https://api.example.com/user/{user_id}', fields: {city: 'data.city'}, capacity: 50, outputMode: 'ordered'},
    'ai.map': {prompt: '把这段投诉归类：${text}', outputField: 'category', system: '你是工单分类助手', agentCode: 'ticket-clf'},
    'field.mapping': {mappings: [{source: 'id', target: 'order_id', type: 'LONG'}]},
    'type.normalize': {rules: [{column: 'amount', type: 'BIGDECIMAL', scale: 2}]},
    'assert': {mode: 'reject', rejectedTo: 'dead', expression: 'amount > 0', notNullRate: {user_id: 1.0}},
    'union': {},
    'explode': {column: 'tags', target: 'tag', indexColumn: 'tag_idx'},
    'topn': {orderBy: 'amount', order: 'desc', limit: 10},
    'limit': {limit: 100, offset: 0},
    'sample': {rate: 0.1, seed: 42},
    'distinct': {columns: ['user']},
    'unpivot': {columns: ['q1', 'q2'], nameColumn: 'quarter', valueColumn: 'amount'},
    'pivot': {keyBy: ['product'], pivot: 'month', value: 'amount', fn: 'SUM'},
    'regex.extract': {column: 'phone_text', pattern: '(\\d{3})-(\\d{4})', outputs: {1: 'area', 2: 'local'}},
    'split': {column: 'full_name', separator: ' ', names: ['first', 'last']},
    'template': {template: '${user}-${order_id}', name: 'biz_key'},
    'rename': {mapping: {user: 'user_name'}},
    'flatten': {separator: '.'},
    'json.extract': {column: 'payload', fields: {'data.city': 'city'}, onError: "'null'"},
    'hash': {columns: ['phone'], algorithm: 'SHA-256', suffix: '_fp'},
    'mask': {masks: {phone: 'phone', email: 'email'}},
    'date.format': {column: 'updated_at', toPattern: 'yyyy-MM-dd HH:mm:ss'},
    'sort': {columns: ['-amount', 'user']},
    'profile': {reportTo: 'profile-report', distinctCap: 10000},
    'aggregate': {keyBy: ['user'], aggregates: [{fn: 'SUM', column: 'amount', alias: 'total'}]},
    'window.aggregate': {keyBy: ['user'], window: {type: 'tumbling', size: '5m'}, allowedLateness: '1m', aggregates: [{fn: 'COUNT', column: '*', alias: 'cnt'}]},
    'dedup': {keyBy: ['order_id'], keep: 'first', ttl: '1h'},
    'join': {left: 'a', right: 'b', leftKey: 'id', rightKey: 'id', joinType: 'inner'},
    'interval.join': {left: 'a', right: 'b', leftKey: 'id', rightKey: 'id', within: '10m'},
    'dim.join': {left: 'main', right: 'dim', leftKey: 'city', rightKey: 'city', select: ['province'], ttl: '1h'},
    'diff': {left: 'cur', right: 'ref', keys: ['order_id'], opColumn: '__op'},
    'except': {left: 'a', right: 'b', keys: ['order_id']},
    'intersect': {left: 'a', right: 'b', keys: ['order_id']},
    'cep.pattern': {keyBy: ['order_id'], steps: [{name: 'create', expression: "type == 'CREATE'"}, {name: 'pay', expression: "type == 'PAID'"}]}
}

export const DEMO_TEXT = [
    'name: demo-',
    'nodes:',
    '  - id: src',
    '    type: inline.source',
    '    config:',
    '      rows:',
    '        - {id: 1, user: 甲, amount: 100}',
    '        - {id: 2, user: 乙, amount: 200}',
    '        - {id: 3, user: 甲, amount: 300}',
    '  - id: uq',
    '    type: distinct',
    '    config:',
    '      columns: [user]',
    '  - id: agg',
    '    type: aggregate',
    '    config:',
    '      keyBy: [user]',
    '      aggregates:',
    '        - {fn: SUM, column: amount, alias: total}',
    '  - id: out',
    '    type: collect.sink',
    'edges:',
    '  - {from: src, to: uq}',
    '  - {from: uq, to: agg}',
    '  - {from: agg, to: out}',
].join('\n')
