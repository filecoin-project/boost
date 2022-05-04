import './styles/theme.scss';
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

function App() {
    return (
        <BrowserRouter>
  			<header>
              <a href="storage-deals.html" class="logo">
                <svg width="150" viewBox="0 0 222 71" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M81.957 50.14h11.415a6.724 6.724 0 0 0 3.446-.918 6.968 6.968 0 0 0 2.525-2.534 6.883 6.883 0 0 0 0-6.881 6.969 6.969 0 0 0-2.525-2.535 6.732 6.732 0 0 0-3.446-.925H81.957V50.14zm13.73-17.478a6.581 6.581 0 0 0 2.356-2.293 5.874 5.874 0 0 0 .879-3.11 6.173 6.173 0 0 0-.856-3.165 6.517 6.517 0 0 0-2.355-2.332 6.018 6.018 0 0 0-3.14-.871H81.957v12.61H92.57a6.075 6.075 0 0 0 3.14-.847l-.023.008zm6.258 15.456a9.613 9.613 0 0 1-3.565 3.515 9.878 9.878 0 0 1-4.985 1.29h-12.09a2.438 2.438 0 0 1-1.735-.645 2.176 2.176 0 0 1-.675-1.648V20.362a2.144 2.144 0 0 1 .675-1.64 2.36 2.36 0 0 1 1.735-.653h11.266a9.12 9.12 0 0 1 4.648 1.244 9.378 9.378 0 0 1 3.399 3.366 8.749 8.749 0 0 1 1.256 4.556 8.11 8.11 0 0 1-1.209 4.354 8.756 8.756 0 0 1-3.266 3.11 9.159 9.159 0 0 1 4.224 3.359 8.896 8.896 0 0 1 1.609 5.201 9.385 9.385 0 0 1-1.311 4.86M113.163 47.652c1.57 1.664 3.69 2.488 6.437 2.488h2.316c2.779 0 4.938-.832 6.485-2.511a9.27 9.27 0 0 0 2.355-6.5V30.12c0-6.172-2.933-9.259-8.8-9.259h-2.316c-5.836 0-8.754 3.087-8.754 9.26v11.01a9.208 9.208 0 0 0 2.355 6.522h-.078zm20.749-17.532v11.01a12.915 12.915 0 0 1-1.413 6.149 9.963 9.963 0 0 1-4.098 4.16 12.985 12.985 0 0 1-6.406 1.492h-2.316a13.042 13.042 0 0 1-6.414-1.492 10.046 10.046 0 0 1-4.098-4.16 12.998 12.998 0 0 1-1.405-6.15V30.12c0-8.028 3.972-12.045 11.917-12.05h2.316c7.945 0 11.917 4.017 11.917 12.05zM143.765 47.652c1.57 1.664 3.69 2.488 6.438 2.488h2.308c2.787 0 4.946-.832 6.492-2.511a9.269 9.269 0 0 0 2.308-6.5V30.12c0-6.172-2.933-9.259-8.8-9.259h-2.308c-5.831 0-8.749 3.087-8.754 9.26v11.01a9.178 9.178 0 0 0 2.355 6.522h-.039zm20.71-17.532v11.01a12.915 12.915 0 0 1-1.413 6.149 10.01 10.01 0 0 1-4.098 4.16 13.018 13.018 0 0 1-6.414 1.492h-2.308a13.015 13.015 0 0 1-6.414-1.492 10.046 10.046 0 0 1-4.098-4.16 12.899 12.899 0 0 1-1.405-6.15V30.12c0-8.028 3.972-12.045 11.917-12.05h2.308c7.945 0 11.92 4.017 11.925 12.05zM169.303 50.606a1.411 1.411 0 0 1 1.029-.427h9.711c2.614 0 4.569-.59 5.88-1.788s1.963-3.11 1.963-5.831a5.53 5.53 0 0 0-.785-3.235 5.076 5.076 0 0 0-2.411-1.765 28.605 28.605 0 0 0-4.71-1.22 39.002 39.002 0 0 1-5.99-1.5 7.847 7.847 0 0 1-3.643-2.66 8.486 8.486 0 0 1-1.436-5.256c0-3.11.957-5.351 2.873-6.725 1.915-1.373 4.663-2.07 8.243-2.091h8.502a1.534 1.534 0 0 1 1.131.427 1.334 1.334 0 0 1 .424.972 1.304 1.304 0 0 1-.424.964 1.408 1.408 0 0 1-1.036.428h-8.636c-2.716 0-4.752.464-6.108 1.392-1.358.933-2.041 2.472-2.041 4.664a5.912 5.912 0 0 0 .981 3.662 5.497 5.497 0 0 0 2.693 1.843c1.629.494 3.293.868 4.977 1.12 1.988.307 3.936.828 5.81 1.555a7.469 7.469 0 0 1 3.391 2.69 8.248 8.248 0 0 1 1.209 4.75c0 3.628-.913 6.27-2.74 7.923-1.826 1.653-4.529 2.467-8.109 2.441h-9.712a1.42 1.42 0 0 1-1.028-.42 1.317 1.317 0 0 1-.318-1.502c.074-.168.182-.318.318-.442M220.552 19.08a1.295 1.295 0 0 1 .456.971 1.27 1.27 0 0 1-.456.965 1.47 1.47 0 0 1-1.052.428h-9.758v30.624a1.359 1.359 0 0 1-.455 1.027 1.438 1.438 0 0 1-1.005.42 1.577 1.577 0 0 1-1.107-.42 1.322 1.322 0 0 1-.448-.972v-30.68h-9.86a1.47 1.47 0 0 1-1.052-.428 1.263 1.263 0 0 1-.335-1.498c.079-.168.193-.317.335-.437a1.451 1.451 0 0 1 1.052-.428h22.531a1.58 1.58 0 0 1 1.154.428zM61.85 46.72c0 1.549-.412 3.07-1.195 4.412a8.867 8.867 0 0 1-3.263 3.23L37.726 65.635a8.97 8.97 0 0 1-8.911 0L9.149 54.393a8.884 8.884 0 0 1-3.262-3.23A8.774 8.774 0 0 1 4.69 46.75V24.258a8.767 8.767 0 0 1 1.196-4.413 8.876 8.876 0 0 1 3.263-3.23l19.666-11.25a9.01 9.01 0 0 1 8.91 0l19.667 11.25a8.862 8.862 0 0 1 3.264 3.229 8.752 8.752 0 0 1 1.195 4.414v22.461zm-.886-32.282L37.836 1.221a9.207 9.207 0 0 0-9.139 0L5.57 14.438a9.11 9.11 0 0 0-3.347 3.312 8.998 8.998 0 0 0-1.23 4.525V48.71a8.99 8.99 0 0 0 1.229 4.525 9.103 9.103 0 0 0 3.348 3.312l23.128 13.217a9.207 9.207 0 0 0 9.139 0l23.128-13.217a9.11 9.11 0 0 0 3.342-3.314 9 9 0 0 0 1.227-4.523V22.275a9.007 9.007 0 0 0-1.228-4.523 9.118 9.118 0 0 0-3.341-3.314z" fill="#fff"/><path d="M49.07 46.61a1.08 1.08 0 0 1-.761-.312 1.06 1.06 0 0 1-.315-.753V28.23l-15.45-8.28a1.103 1.103 0 0 1-.565-.925 1.073 1.073 0 0 1 .541-.94l3.337-1.913a1.1 1.1 0 0 1 1.052 0l16.039 8.599a1.07 1.07 0 0 1 .565.933v17.905a1.073 1.073 0 0 1-.534.925l-3.36 1.92a1.1 1.1 0 0 1-.542.14" fill="#FFC844"/><path d="M37.01 53.53c-.236.002-.47-.06-.674-.179a1.337 1.337 0 0 1-.675-1.15V35.04L20.35 26.84a1.344 1.344 0 0 1-.689-1.166 1.327 1.327 0 0 1 .69-1.167l7.065-4.035a1.368 1.368 0 0 1 1.32 0l16.03 8.6a1.352 1.352 0 0 1 .707 1.173v17.929a1.32 1.32 0 0 1-.675 1.158l-7.066 4.043c-.204.12-.438.182-.675.18" fill="#B92454"/></svg>
              </a>
              <button type="button" class="navbar-toggle collapsed">
                <span class="visually-hidden">Toggle Navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
              </button>
              <Epoch />
  			</header>

            <main>
                <Menu />
                <div className="main-wrapper">
                    <Routes>
                        <Route path="/storage-deals" element={<StorageDealsPage />} />
                        <Route path="/storage-deals/from/:cursor/page/:pageNum" element={<StorageDealsPage />} />
                        <Route path="/legacy-storage-deals" element={<LegacyStorageDealsPage />} />
                        <Route path="/legacy-storage-deals/from/:cursor/page/:pageNum" element={<LegacyStorageDealsPage />} />
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
                        <Route path="/" element={<StorageDealsPage />} />
                    </Routes>
                </div>
            </main>
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
