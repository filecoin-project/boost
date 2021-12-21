import './App.css';
import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom'
import {ApolloProvider} from "@apollo/react-hooks";
import {gqlClient,} from "./gql";
import {StorageSpacePage} from "./StorageSpace";
import {FundsPage} from "./Funds";
import {StorageDealsPage} from "./Deals";
import {SealingPipelinePage} from "./SealingPipeline";
import {DealPublishPage} from "./DealPublish";
import {DealTransfersPage} from "./DealTransfers"
import {MpoolPage} from "./Mpool";
import {DealDetail} from "./DealDetail";

function App(props) {
    return (
        <BrowserRouter>
            <div id="content">
                <table className="content-table">
                    <tbody>
                        <tr>
                            <Menu />
                            <td className="page-content">
                                <Routes>
                                    <Route path="/storage-deals" element={<StorageDealsPage />} />
                                    <Route path="/storage-space" element={<StorageSpacePage />} />
                                    <Route path="/sealing-pipeline" element={<SealingPipelinePage />} />
                                    <Route path="/funds" element={<FundsPage />} />
                                    <Route path="/deal-publish" element={<DealPublishPage />} />
                                    <Route path="/deal-transfers" element={<DealTransfersPage />} />
                                    <Route path="/mpool" element={<MpoolPage />} />
                                    <Route path="/deals/:dealID" element={<DealDetail />} />
                                    <Route path="/" element={<StorageDealsPage />} />
                                </Routes>
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

var pages = [{
    title: 'Storage Deals',
    pageType: 'storage-deals',
}, {
    title: 'Storage Space',
    pageType: 'storage-space',
}, {
    title: 'Sealing Pipeline',
    pageType: 'sealing-pipeline',
}, {
    title: 'Funds',
    pageType: 'funds',
}, {
    title: 'Publish Deals',
    pageType: 'deal-publish',
}, {
    title: 'Deal Transfers',
    pageType: 'deal-transfers',
}, {
    title: 'Message Pool',
    pageType: 'mpool',
}]

function Menu(props) {
    return (
        <td className="menu">
            {pages.map(page => (
                <Link key={page.pageType} className="menu-item" to={page.pageType}>
                    {page.title}
                </Link>
            ))}
        </td>
    )
}

export default AppRoot;
