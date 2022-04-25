import {React, useEffect, useState} from "react";
import InfoIcon from "bootstrap-icons/icons/info-circle.svg"

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
            <img className={"target filter-" + props.color || "primary"} src={InfoIcon} alt="Click for more Info" onClick={() => setShow(!show)} />
            {/* <div className="target" onClick={() => setShow(!show)}></div> */}
            { show ? <div className="content">{props.children}</div> : null }
        </div>
    )
}
