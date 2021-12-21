import React from "react";
import {Link} from "react-router-dom";

export function PageContainer(props) {
    return (
        <div id={props.pageType}>
            <div className="page-title">{props.title}</div>
            {props.children}
        </div>
    )
}

export function ShortDealLink(props) {
    return <Link to={"/deals/" + props.id}>
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
