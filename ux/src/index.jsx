'use strict'

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

var now = new Date().getTime()

var storageDealsPageData = {
  title: 'Storage Deals',
  pageType: 'storage-deals',
  deals: [{
    start: new Date(now-(2*60*1000)),
    dealID: 12345,
    size: '32G',
    amount: 0.3,
    provider: 'f0127896',
    state: 'Transferring',
    stateText: 'Transferring - 20%',
    logs: [{
      at: new Date(now-(2*60*1000)-50.128*1000),
      text: 'Propose Deal'
    }, {
      at: new Date(now-(2*60*1000)-49.935*1000),
      text: 'Accepted'
    }, {
      at: new Date(now-(2*60*1000)-49.432*1000),
      text: 'Start Data Transfer'
    }]
  }, {
    start: new Date(now-(4*60*1000)),
    dealID: 122334,
    size: '32G',
    amount: 0.25,
    provider: 'f054321',
    state: 'Transferring',
    stateText: 'Transferring - 38%',
    logs: [{
      at: new Date(now-(4*60*1000)-50.198*1000),
      text: 'Propose Deal'
    }, {
      at: new Date(now-(4*60*1000)-49.835*1000),
      text: 'Accepted'
    }, {
      at: new Date(now-(4*60*1000)-49.282*1000),
      text: 'Start Data Transfer'
    }]
  }, {
    start: new Date(now-(2*60*60*1000)),
    dealID: 423422,
    size: '64G',
    amount: 0.4,
    provider: 'f02341',
    state: 'SealingQueued',
    stateText: 'Queued for sealing',
    logs: [{
      at: new Date(now-(2*60*60*1000)-(60*60*1000 + 50.522*1000)),
      text: 'Propose Deal'
    }, {
      at: new Date(now-(2*60*60*1000)-(60*60*1000 + 49.825*1000)),
      text: 'Accepted'
    }, {
      at: new Date(now-(2*60*60*1000)-(60*60*1000 + 49.192*1000)),
      text: 'Start Data Transfer'
    }, {
      at: new Date(now-(2*60*60*1000)-(55*60*1000 + 0.234*1000)),
      text: 'Data Transfer Complete'
    }, {
      at: new Date(now-(2*60*60*1000)-(47*60*1000 + 0.943*1000)),
      text: 'Deal Published'
    }]
  }, {
    start: new Date(now-(3*60*60*1000)),
    dealID: 424322,
    size: '32G',
    amount: 0.3,
    provider: 'f043120',
    state: 'Sealing',
    stateText: 'Sealing',
    logs: [{
      at: new Date(now-(3*60*60*1000)-(60*60*1000 + 50.825*1000)),
      text: 'Propose Deal'
    }, {
      at: new Date(now-(3*60*60*1000)-(60*60*1000 + 49.192*1000)),
      text: 'Accepted'
    }, {
      at: new Date(now-(3*60*60*1000)-(60*60*1000 + 49.032*1000)),
      text: 'Start Data Transfer'
    }, {
      at: new Date(now-(3*60*60*1000)-(55*60*1000 + 0.311*1000)),
      text: 'Data Transfer Complete'
    }, {
      at: new Date(now-(3*60*60*1000)-(47*60*1000 + 0.432*1000)),
      text: 'Deal Published'
    }, {
      at: new Date(now-(3*60*60*1000)-(13*60*1000 + 0.932*1000)),
      text: 'Deal Pre-committed'
    }]
  }, {
    start: new Date(now-(5*60*60*1000)),
    dealID: 427322,
    size: '64G',
    amount: 0.62,
    provider: 'f0423420',
    state: 'Error',
    stateText: 'Error - Connection Lost',
    logs: [{
      at: new Date(now-(5*60*60*1000)-(60*60*1000 + 50.243*1000)),
      text: 'Propose Deal'
    }, {
      at: new Date(now-(5*60*60*1000)-(60*60*1000 + 49.642*1000)),
      text: 'Accepted'
    }, {
      at: new Date(now-(5*60*60*1000)-(60*60*1000 + 49.234*1000)),
      text: 'Start Data Transfer'
    }, {
      at: new Date(now-(5*60*60*1000)-(55*60*1000 + 0.923*1000)),
      text: 'Error - Connection lost'
    }]
  }, {
    start: new Date(now-(6*60*60*1000)),
    dealID: 232922,
    size: '32G',
    amount: 0.39,
    provider: 'f08234',
    state: 'Error',
    stateText: 'Error - Not enough funds',
    logs: [{
      at: new Date(now-(6*60*60*1000)-(60*60*1000 + 50.423*1000)),
      text: 'Propose Deal'
    }, {
      at: new Date(now-(6*60*60*1000)-(60*60*1000 + 49.932*1000)),
      text: 'Accepted'
    }, {
      at: new Date(now-(6*60*60*1000)-(60*60*1000 + 49.432*1000)),
      text: 'Start Data Transfer'
    }, {
      at: new Date(now-(6*60*60*1000)-(47*60*1000 + 0.311*1000)),
      text: 'Data Transfer Complete'
    }, {
      at: new Date(now-(6*60*60*1000)-(46*60*1000 + 59.432*1000)),
      text: 'Publishing Deal'
    }, {
      at: new Date(now-(6*60*60*1000)-(46*60*1000 + 59.123*1000)),
      text: 'Error - Not enough funds'
    }]
  }]
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

const e = React.createElement;

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

class DealLog extends React.Component {
  render() {
    var prev = this.props.prev
    var log = this.props.log
    var sinceLast = ''
    if (prev != null) {
      var logMs = log.at.getTime()
      var prevMs = prev.at.getTime()
      if (logMs - prevMs < 1000) {
        sinceLast = (logMs - prevMs) + 'ms'
      } else {
        sinceLast = moment(prev.at).from(log.at)
      }
    }

    return  <tr>
      <td>{moment(log.at).format(dateFormat)}</td>
      <td className="since-last">{sinceLast}</td>
      <td>{log.text}</td>
    </tr>
  }
}

class DealDetail extends React.Component {
  render() {
    var deal = this.props.deal

    var logRowData = []
    for (var i = 0; i < (deal.logs || []).length; i++) {
      var log = deal.logs[i]
      var prev = i == 0 ? null : deal.logs[i-1]
      logRowData.push({ log: log, prev: prev })
    }

    return <div className="deal-detail" id={deal.dealID} style={ this.props.show ? null : {display: 'none'} } >
      <div className="title">Deal {deal.dealID}</div>
      <table className="deal-fields">
        <tbody>
          <tr>
            <td>Start</td>
            <td>{moment(deal.start).format(dateFormat)}</td>
          </tr>
          <tr>
            <td>Amount</td>
            <td>{deal.amount+' FIL'}</td>
          </tr>
          <tr>
            <td>Size</td>
            <td>{deal.size}</td>
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

class StorageDealsPage extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      dealToShow: null,
    }
  }

  render() {
    return <div className="deals">
      <table>
        <tbody>
          <tr>
            <th>Start</th>
            <th>Deal ID</th>
            <th>Size</th>
            <th>Provider</th>
            <th>State</th>
          </tr>

          {this.props.deals.map(deal => (
            <tr key={deal.dealID} onClick={() => this.setState({ dealToShow: deal.dealID })}>
              <td>{moment(deal.start).fromNow()}</td>
              <td>{deal.dealID}</td>
              <td>{deal.size}</td>
              <td>{deal.provider}</td>
              <td>{deal.stateText}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <div id="deal-detail">
        { this.props.deals.map(deal => <DealDetail key={deal.dealID} deal={deal} show={ this.state.dealToShow == deal.dealID } />) }
      </div>
    </div>
  }
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
            <div>
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
          <div key={page.pageType} id={page.pageType} style={this.props.pageToShow == page.pageType ? {} : {display: 'none'}}>
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
    }
  }
}

class App extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      pageToShow: 'storage-deals',
    }
  }

  render() {
    return (
      <table className="content-table">
        <tbody>
          <tr>
            <Menu pages={pages} pageToShow={this.state.pageToShow} onMenuItemClick={(pageType) => this.setState({ pageToShow: pageType })} />
            <Pages pages={pages} pageToShow={this.state.pageToShow} />
          </tr>
        </tbody>
      </table>
    )
  }
}

const domContainer = document.querySelector('#content')
ReactDOM.render(e(App), domContainer)
