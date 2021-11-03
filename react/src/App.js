import './App.css';
import moment from 'moment';
import React, { useState } from 'react';
import ApolloClient from "apollo-client";
import { ApolloProvider } from "@apollo/react-hooks";
import { WebSocketLink } from "apollo-link-ws";
import { InMemoryCache } from "apollo-cache-inmemory";
import { useSubscription, useQuery } from "./hooks";
import gql from "graphql-tag";

const gqlClient = new ApolloClient({
    link: new WebSocketLink({
        uri: "ws://localhost:8080/graphql",
        options: {
            reconnect: true,
        },
    }),
    cache: new InMemoryCache(),
});

const DealsListQuery = gql`
    query AppDealsListQuery {
      deals {
        ID
        CreatedAt
        PieceCid
        PieceSize
        Client
        State
        Logs {
          CreatedAt
          Text
        }
      }
    }
`;

const DealSubscription = gql`
    subscription AppDealSubscription($id: ID!) {
      dealSub(id: $id) {
        ID
        CreatedAt
        PieceCid
        PieceSize
        Client
        State
        Logs {
          CreatedAt
          Text
        }
      }
    }
`;

moment.locale('en', {
    relativeTime: {
        future: "in %s",
        past:   "%s",
        s  : '%ds',
        ss : '%ds',
        m:  "1m",
        mm: "%dm",
        h:  "1h",
        hh: "%dh",
        d:  "a day",
        dd: "%d days",
        w:  "a week",
        ww: "%d weeks",
        M:  "a month",
        MM: "%d months",
        y:  "a year",
        yy: "%d years"
    }
})

var dateFormat = 'YYYY-MM-DD HH:mm:ss.SSS'

var storageDealsPageData = {
    title: 'Storage Deals',
    pageType: 'storage-deals',
}

var storageSpacePageData = {
    title: 'Storage Space',
    pageType: 'storage-space',
    usage: {
        free: {
            name: 'Free',
            size: 8.5
        },
        transferring: {
            name: 'Transferring',
            size: 5.2,
            fill: 3.5
        },
        queued: {
            name: 'Queued for sealing',
            size: 11.2
        }
    }
}

var pages = [
    storageDealsPageData,
    storageSpacePageData
]

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

class Menu extends React.Component {
    render() {
        return (
            <td className="menu">
                {this.props.pages.map(page => (
                    <div key={page.pageType} className="menu-item" onClick={() => this.props.onMenuItemClick(page.pageType)}>
                        {page.title}
                    </div>
                ))}
            </td>
        )
    }
}

function DealLog(props) {
    var prev = props.prev
    var log = props.log
    var sinceLast = ''
    if (prev != null) {
        var logMs = log.CreatedAt.getTime()
        var prevMs = prev.CreatedAt.getTime()
        if (logMs - prevMs < 1000) {
            sinceLast = (logMs - prevMs) + 'ms'
        } else {
            sinceLast = moment(prev.CreatedAt).from(log.CreatedAt)
        }
    }

    return  <tr>
        <td>{moment(log.CreatedAt).format(dateFormat)}</td>
        <td className="since-last">{sinceLast}</td>
        <td>{log.Text}</td>
    </tr>
}

class DealDetail extends React.Component {
    render() {
        var deal = this.props.deal

        var logRowData = []
        for (var i = 0; i < (deal.Logs || []).length; i++) {
            var log = deal.Logs[i]
            var prev = i === 0 ? null : deal.Logs[i-1]
            logRowData.push({ log: log, prev: prev })
        }

        return <div className="deal-detail" id={deal.ID} style={ this.props.show ? null : {display: 'none'} } >
            <div className="title">Deal {deal.ID}</div>
            <table className="deal-fields">
                <tbody>
                <tr>
                    <td>CreatedAt</td>
                    <td>{moment(deal.CreatedAt).format(dateFormat)}</td>
                </tr>
                <tr>
                    <td>Amount</td>
                    <td>{'0.2 FIL'}</td>
                </tr>
                <tr>
                    <td>Size</td>
                    <td>{deal.PieceSize}</td>
                </tr>
                </tbody>
            </table>

            <div className="buttons">
                <div className="button cancel">Cancel</div>
                <div className="button retry">Retry</div>
            </div>

            <table className="deal-logs">
                <tbody>
                {logRowData.map((l, i) => <DealLog key={i} log={l.log} prev={l.prev} />)}
                </tbody>
            </table>
        </div>
    }
}

function DealRow(props) {
    const { loading, error, data } = useSubscription(DealSubscription, {
        variables: { id: props.deal.ID }
    })

    if (error) {
        return <tr><td>Error: {error}</td></tr>
    }

    var deal = props.deal
    if (!loading) {
        deal = data.dealSub
    }

    return (
        <tr onClick={() => props.onDealRowClick(deal.ID)}>
            <td>{moment(deal.CreatedAt).fromNow()}</td>
            <td>{deal.ID}</td>
            <td>{humanFileSize(deal.PieceSize, false, 0)}</td>
            <td>{deal.Client}</td>
            <td>{deal.State}</td>
        </tr>
    )
}

function StorageDealsPage(props) {
    const [dealToShow, setDealToShow] = useState(null)
    const { loading, error, data } = useQuery(DealsListQuery)

    if (loading) return <div>Loading...</div>;
    if (error) return <div>Error: {error.message}</div>;

    return <div className="deals">
        <table>
            <tbody>
                <tr>
                    <th>Start</th>
                    <th>Deal ID</th>
                    <th>Size</th>
                    <th>Client</th>
                    <th>State</th>
                </tr>

                { data.deals.map(deal => (
                    <DealRow key={deal.ID} deal={deal} onDealRowClick={() => setDealToShow(deal.ID) }></DealRow>
                ))}

            </tbody>
        </table>

        <div id="deal-detail">
            { data.deals.map(deal => (
                <DealDetail key={deal.ID} deal={deal} show={ dealToShow === deal.ID } />
            ))}
        </div>
    </div>
}

class StorageBar extends React.Component {
    render() {
        var tp = this.props.barType
        var barHeightRatio = this.props.usage[tp].size / this.props.max
        var barHeight = Math.round(barHeightRatio * 100)
        var fillerHeight = 100 - barHeight

        return <td className={'storage-bar ' + this.props.barType}>
            <div className="filler" style={{ height: fillerHeight+'%' }}></div>
            <div className="bar" style={{ height: barHeight+'%' }}>
                <div className="size">{this.props.usage[tp].size} GB</div>
                { this.props.used ? (
                    <div style={{ height: '100%' }}>
                        <div className="unused" style={{ height: (100 - this.props.used)+'%' }} />
                        <div className="used" style={{ height: this.props.used+'%' }} />
                    </div>
                ) : null}
            </div>
        </td>
    }
}

class StorageSpacePage extends React.Component {
    render() {
        var max = 0
        var types = ['free', 'transferring', 'queued']
        for (let tp of types) {
            if (this.props.usage[tp].size > max) {
                max = this.props.usage[tp].size
            }
        }
        var tfrUsed = Math.round(100 * this.props.usage.transferring.fill / this.props.usage.transferring.size)

        return <div className="storage">
            <table>
                <tbody>
                <tr>
                    <StorageBar usage={this.props.usage} max={max} barType="free" />
                    <StorageBar usage={this.props.usage} max={max} barType="transferring" used={tfrUsed} />
                    <StorageBar usage={this.props.usage} max={max} barType="queued" />
                </tr>
                <tr>
                    <td className="label">Free</td>
                    <td className="label">Transferring</td>
                    <td className="label">Queued</td>
                </tr>
                </tbody>
            </table>
        </div>
    }
}

class Pages extends React.Component {
    render() {
        return (
            <td>
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
                return <StorageDealsPage key={page.pageType} deals={page.deals} />
            case 'storage-space':
                return <StorageSpacePage key={page.pageType} usage={page.usage} />
            default:
                throw new Error("unrecognized page type " + page.pageType)
        }
    }
}

// TODO: Check licensing
// Copied from
// https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable-string
function humanFileSize(bytes, si=false, dp=1) {
    const thresh = si ? 1000 : 1024;

    if (Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }

    const units = si
        ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    const r = 10**dp;

    do {
        bytes /= thresh;
        ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);

    return bytes.toFixed(dp) + ' ' + units[u];
}

export default AppRoot;