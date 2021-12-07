import {React, useEffect, useState} from "react";
import "./Info.css"

export function Info(props) {
    const [show, setShow] = useState(false)
    useEffect(() => {
        const onClick = () => show && setShow(false)
        document.body.addEventListener('click', onClick)
        return function () {
            document.body.removeEventListener('click', onClick)
        }
    })

    return (
        <div className="info">
            <div className="target" onClick={() => setShow(!show)}>i</div>
            { show ? <div className="content">{props.children}</div> : null }
        </div>
    )
}
