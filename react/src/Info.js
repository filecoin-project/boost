import {React, useEffect, useState} from "react";
import InfoIcon from "bootstrap-icons/icons/info-circle.svg"
import InfoIconWhite from "bootstrap-icons/icons/info-circle-white.svg"

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

    const icon = props.invertColor ? InfoIconWhite : InfoIcon
    return (
        <div className={"info " + (props.className || "")}>
            <img className={"target filter-" + props.color || "primary"} src={icon} alt="Click for more Info" onClick={() => setShow(!show)} />
            { show ? <div className="content">{props.children}</div> : null }
        </div>
    )
}
