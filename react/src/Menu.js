import {NavLink} from "react-router-dom";
import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";
import gridImg from './bootstrap-icons/icons/grid-3x3-gap.svg'
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
                    <NavLink key="mpool" className="sidebar-item sidebar-item-deal-transfers" to="/mpool">
                        <span className="sidebar-icon">
                            <img className="icon" alt="Message Pool Icon" src={gridImg} />
                        </span>
                        <span className="sidebar-title">Message Pool</span>
                    </NavLink>
                    <SettingsMenuItem />
                </div>
            </div>
        </div>
    )
}

function scrollToTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
