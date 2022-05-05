import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";
import {SettingsMenuItem} from "./Settings";
import {MpoolMenuItem} from "./Mpool";

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
                    <MpoolMenuItem />
                    <SettingsMenuItem />
                </div>
            </div>
        </div>
    )
}

function scrollToTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
