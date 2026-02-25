import {Link} from "react-router-dom";
import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";
import gridImg from './bootstrap-icons/icons/grid-3x3-gap.svg'
import './Menu.css'
import {SettingsMenuItem} from "./Settings";
import {LIDMenuItem} from "./LID";
import {ProposalLogsMenuItem} from "./ProposalLogs";
import {RetrievalLogsMenuItem} from "./RetrievalLogs";
import {IpniMenuItem} from "./Ipni";

export function Menu(props) {
    function scrollToTop() {
        window.scrollTo({ top: 0, behavior: "smooth" })
    }

    return (
        <td onClick={scrollToTop} className="menu">
            <StorageDealsMenuItem />
            <ProposalLogsMenuItem />
            <RetrievalLogsMenuItem />
            <LIDMenuItem />
            <SealingPipelineMenuItem />
            <DealPublishMenuItem />
            <IpniMenuItem />
            <StorageSpaceMenuItem />
            <FundsMenuItem />
            <DealTransfersMenuItem />
            <Link key="mpool" className="menu-item" to="/mpool">
                <img className="icon" alt="" src={gridImg} />
                <h3>Message Pool</h3>
            </Link>
            <SettingsMenuItem />
        </td>
    )
}

