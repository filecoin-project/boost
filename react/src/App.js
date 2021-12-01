import './App.css';
import React, {useState} from 'react';
import {ApolloProvider} from "@apollo/react-hooks";
import {gqlClient,} from "./gql";
import {StorageSpacePage} from "./StorageSpace";
import {FundsPage} from "./Funds";
import {StorageDealsPage} from "./Deals";
import {DealPublishPage} from "./DealPublish";

function App(props) {
    const [pageToShow, setPageToShow] = useState('storage-deals');

    return (
        <div id="content">
            <table className="content-table">
                <tbody>
                <tr>
                    <Menu pages={pages} pageToShow={pageToShow} onMenuItemClick={setPageToShow} />
                    <Pages pages={pages} pageToShow={pageToShow} />
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function AppRoot(props) {
    return (
        <ApolloProvider client={gqlClient}>
            <App />
        </ApolloProvider>
    );
}

function Menu(props) {
    return (
        <td className="menu">
            {props.pages.map(page => (
                <div key={page.pageType} className="menu-item" onClick={() => props.onMenuItemClick(page.pageType)}>
                    {page.title}
                </div>
            ))}
        </td>
    )
}

var pages = [{
        title: 'Storage Deals',
        pageType: 'storage-deals',
    }, {
        title: 'Storage Space',
        pageType: 'storage-space',
    }, {
        title: 'Funds',
        pageType: 'funds',
    }, {
        title: 'Deal Publish',
        pageType: 'deal-publish',
    }
]

class Pages extends React.Component {
    render() {
        return (
            <td class="page-content">
                {this.props.pages.map(page => (
                    <div key={page.pageType} id={page.pageType} style={this.props.pageToShow === page.pageType ? {} : {display: 'none'}}>
                        <div className="page-title">{page.title}</div>
                        <div className="page-content">{this.renderPage(page)}</div>
                    </div>
                ))}
            </td>)
    }

    renderPage(page) {
        switch (page.pageType) {
            case 'storage-deals':
                return <StorageDealsPage key={page.pageType} />
            case 'storage-space':
                return <StorageSpacePage key={page.pageType} />
            case 'funds':
                return <FundsPage key={page.pageType} />
            case 'deal-publish':
                return <DealPublishPage key={page.pageType} />
            default:
                throw new Error("unrecognized page type " + page.pageType)
        }
    }
}

export default AppRoot;