import {React, useEffect, useState} from "react";
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

    return (
        <div className={"info " + props.className || ""}>
            <div className="target" onClick={() => setShow(!show)}></div>
            { show ? <div className="content">{props.children}</div> : null }
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
