
export function addClassFor(el, className, duration) {
    el.classList.add(className)
    return setTimeout(function () {
        el.classList.remove(className)
    }, duration)
}