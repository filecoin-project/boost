import {React, useEffect, useState, useRef} from "react";
import "./Info.css"

export function Info(props) {
    return <InfoBox {...props} />
}

export function Warn(props) {
    return <InfoBox {...props} className="warning" />
}

function InfoBox(props) {
    const [show, setShow] = useState(false)
    useEffect(() => {
        const onClick = () => show && setShow(false)
        document.body.addEventListener('click', onClick)
        return function () {
            document.body.removeEventListener('click', onClick)
        }
    })

    const contentRef = useRef(null)
    useEffect(() => {
        if (contentRef.current) {
            // If the Info box content goes over the right edge of the screen
            // add a class to the css that will switch it to be on the left of
            // the Info target (i)
            const rect = contentRef.current.getBoundingClientRect()
            if (rect.width === 0) {
                return
            }
            if (rect.right > document.documentElement.clientWidth) {
                contentRef.current.classList.add('contain-right')
            }
        }
    });

    return (
        <div className={"info " + props.className || ""}>
            <div className="target" onClick={() => setShow(!show)}></div>
            <div ref={contentRef} className={"content" + (show ? ' show' : '')}>{props.children}</div>
        </div>
    )
}

export function InfoListItem(props) {
    return <div className="info-item">
        <div className="info-title">{props.title}</div>
        <div className="item-content">
            {props.children}
        </div>
    </div>
}
