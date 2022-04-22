import {BrowserRouter, Link} from "react-router-dom";
import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";
import gridImg from './bootstrap-icons/icons/grid-3x3-gap.svg'
// import './Menu.css'
import {SettingsMenuItem} from "./Settings";

export function Menu() {
    return (
        <div onClick={scrollToTop} className="sidebar">
            <div className="sidebar-wrapper">
                <div className="sidebar-menu">
                    <StorageDealsMenuItem />
                    <StorageSpaceMenuItem />
                    <SealingPipelineMenuItem />
                    <FundsMenuItem />
                    <DealPublishMenuItem />
                    <DealTransfersMenuItem />
                    <Link key="mpool" className="sidebar-item sidebar-item-deal-transfers active" to="/mpool">
                        <img className="icon" alt="" src={gridImg} />
                        <span className="sidebar-title">Message Pool</span>
                    </Link>
                    <SettingsMenuItem />
                </div>
            </div>
        </div>
        //
        // <td onClick={scrollToTop} className="menu">
        // </td>
    )
}

function scrollToTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
