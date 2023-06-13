import './App.css';
import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import {ApolloProvider} from "@apollo/react-hooks";
import {gqlClient,} from "./gql";
import {Menu} from "./Menu";
import {StorageSpacePage} from "./StorageSpace";
import {FundsPage} from "./Funds";
import {StorageDealsPage} from "./Deals";
import {LegacyStorageDealsPage} from "./LegacyDeals";
import {SealingPipelinePage} from "./SealingPipeline";
import {DealPublishPage} from "./DealPublish";
import {DealTransfersPage} from "./DealTransfers"
import {MpoolPage} from "./Mpool";
import {DealDetail} from "./DealDetail";
import {Epoch} from "./Epoch";
import {LegacyDealDetail} from "./LegacyDealDetail"
import {SettingsPage} from "./Settings";
import {Banner} from "./Banner";
import {ProposalLogsPage} from "./ProposalLogs";
import {InspectPage} from "./Inspect";
import {RetrievalLogsPage} from "./RetrievalLogs";
import {RetrievalLogDetail} from "./RetrievalLogDetail";
import {MonitoringAlert} from "./MonitoringAlert";

function App(props) {
    return (
        <BrowserRouter>
            <div id="content">
                <table className="content-table">
                    <tbody>
                        <tr>
                            <Menu />
                            <td className="main-content">
                                <div className="page-content">
                                    <MonitoringAlert />
                                    <Epoch />
                                    <Banner />
                                    <Routes>
                                        <Route path="/storage-deals" element={<StorageDealsPage />} />
                                        <Route path="/storage-deals/from/:cursor/page/:pageNum" element={<StorageDealsPage />} />
                                        <Route path="/legacy-storage-deals" element={<LegacyStorageDealsPage />} />
                                        <Route path="/legacy-storage-deals/from/:cursor/page/:pageNum" element={<LegacyStorageDealsPage />} />
                                        <Route path="/proposal-logs" element={<ProposalLogsPage />} />
                                        <Route path="/proposal-logs/from/:cursor/page/:pageNum" element={<ProposalLogsPage />} />
                                        <Route path="/retrieval-logs" element={<RetrievalLogsPage />} />
                                        <Route path="/retrieval-logs/from/:cursor/page/:pageNum" element={<RetrievalLogsPage />} />
                                        <Route path="/retrieval-logs/:peerID/:transferID" element={<RetrievalLogDetail />} />
                                        <Route path="/storage-space" element={<StorageSpacePage />} />
                                        <Route path="/sealing-pipeline" element={<SealingPipelinePage />} />
                                        <Route path="/funds" element={<FundsPage />} />
                                        <Route path="/funds/from/:cursor/page/:pageNum" element={<FundsPage />} />
                                        <Route path="/deal-publish" element={<DealPublishPage />} />
                                        <Route path="/deal-transfers" element={<DealTransfersPage />} />
                                        <Route path="/mpool" element={<MpoolPage />} />
                                        <Route path="/settings" element={<SettingsPage />} />
                                        <Route path="/deals/:dealID" element={<DealDetail />} />
                                        <Route path="/legacy-deals/:dealID" element={<LegacyDealDetail />} />
                                        <Route path="/inspect" element={<InspectPage />} />
                                        <Route path="/inspect/:query" element={<InspectPage />} />
                                        <Route path="/" element={<StorageDealsPage />} />
                                    </Routes>
                                </div>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </BrowserRouter>
    )
}

function AppRoot(props) {
    return (
        <ApolloProvider client={gqlClient}>
            <App />
        </ApolloProvider>
    );
}

export default AppRoot;
