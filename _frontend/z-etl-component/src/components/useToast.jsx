import {useCallback, useRef, useState} from 'react'

/** 控制台右下角 toast 的 React 版（各视图自持，互不干扰）。 */
export function useToast() {
    const [t, setT] = useState(null)
    const timer = useRef(null)
    const notify = useCallback((msg, ok = true) => {
        setT({msg, ok})
        clearTimeout(timer.current)
        timer.current = setTimeout(() => setT(null), 4000)
    }, [])
    const toastEl = t ? <div className={'zetl-toast ' + (t.ok ? 'ok' : 'err')}>{t.msg}</div> : null
    return {notify, toastEl}
}
