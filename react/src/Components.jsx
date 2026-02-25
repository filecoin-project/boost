/* global BigInt */
import React, {useState} from "react";
import {Link} from "react-router-dom";
import {addCommas} from "./util";

export function PageContainer(props) {
    return (
        <div id={props.pageType}>
            <div className="page-title">{props.title}</div>
            {props.children}
        </div>
    )
}

export function ShortDealLink(props) {
    let linkBase = "/deals/"
    if (props.isLegacy){
        linkBase = "/legacy-deals/"
        return <Link to={linkBase + props.id}>
            <ShortDealID id={props.id} />
        </Link>
    }
    if (props.isDirect){
        linkBase = "/direct-deals/"
    }
    return <Link to={linkBase + props.id}>
        <ShortDealID id={props.id} />
    </Link>
}

export function ShortDealID(props) {
    const shortId = props.id.substring(0, 8) + '…'
    return <div className="short-deal-id">{shortId}</div>
}

export function ShortClientAddress(props) {
    const shortAddr = props.address.substring(0, 8) + '…'
    return <div>{shortAddr}</div>
}

export function ShortPeerID(props) {
    const shortPeerId = '…' + props.peerId.slice(-8)
    return <div>{shortPeerId}</div>
}

export function ShortCID(props) {
    const cid = props.cid.substring(0, 8) + '…'
    return <div>{cid}</div>
}

export function ShortCIDTail(props) {
    const cid = '…' + props.cid.slice(-8)
    return <div>{cid}</div>
}

export function ExpandableJSObject(props) {
    const [expanded, setExpanded] = useState(props.expanded || false)

    var val = props.v
    const isObject = (val && typeof val === 'object')
    if (isObject) {
        const isArray = Array.isArray(val)
        val = Object.keys(val).sort().map(ck => <ExpandableJSObject k={ck} v={val[ck]} key={ck} arrEl={isArray} />)
    } else if ((typeof val === 'string' || typeof val === 'number') && (''+val).match(/^[0-9]+$/)) {
        val = addCommas(BigInt(val))
    } else if (typeof val == "boolean") {
        val = val + ''
    }

    function toggleExpandState() {
        setExpanded(!expanded)
    }

    const expandable = isObject && props.topLevel
    return (
        <div className={"js-obj" + (expandable ? ' expandable' : '') + (expanded ? ' expanded' : '')}>
            {props.k && !props.arrEl ? (
                <>
                <span className="js-obj-name" onClick={toggleExpandState}>
                    {props.k}:
                    {expandable ? (
                        <div className="expand-collapse"></div>
                    ) : null}
                </span>
                &nbsp;
                </>
            ) : null}
            <span className="js-obj-val">{val}</span>
        </div>
    )
}
