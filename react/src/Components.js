import React from "react";
import {Link} from "react-router-dom";
import {Banner} from "./Banner";

export function PageContainer(props) {
    return (<>
        <div className="page-header">
            <span className="page-header-icon">{props.icon}</span>
            <h1>{props.title}</h1>
        </div>
        <div className="page-content">
            <Banner />
            {props.children}
        </div>
    </>)
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
