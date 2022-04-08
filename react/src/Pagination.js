import {Link} from "react-router-dom";
import "./Pagination.css"

export function Pagination({basePath, cursor, pageNum, moreRows, totalCount, rowsPerPage, onRowsPerPageChange, onLinkClick}) {
    var totalPages = Math.ceil(totalCount / rowsPerPage)

    var pageLinks = {}
    if (cursor) {
        if (pageNum === 2) {
            pageLinks.prev = basePath
        } else if (pageNum > 2) {
            pageLinks.prev = basePath + '/from/' + cursor + '/page/' + (pageNum - 1)
        }

        if (moreRows) {
            pageLinks.next = basePath + '/from/' + cursor + '/page/' + (pageNum + 1)
        }
    }

    return (
        <div className="pagination">
            <div className="controls">
                {pageNum > 1 ? (
                    <Link className="first" to={basePath} onClick={onLinkClick}>&lt;&lt;</Link>
                ) : <span className="first">&lt;&lt;</span>}
                {pageLinks.prev ? <Link to={pageLinks.prev} onClick={onLinkClick}>&lt;</Link> : <span>&lt;</span>}
                <div className="page">{pageNum} of {totalPages}</div>
                {pageLinks.next ? <Link to={pageLinks.next} onClick={onLinkClick}>&gt;</Link> : <span>&gt;</span>}
                <div className="total">{totalCount} deals</div>
                {onRowsPerPageChange ? (
                    <div className="per-page">
                        <select value={rowsPerPage} onChange={onRowsPerPageChange}>
                            <option value={10}>10 pp</option>
                            <option value={25}>25 pp</option>
                            <option value={50}>50 pp</option>
                            <option value={100}>100 pp</option>
                        </select>
                    </div>
                ) : null}
            </div>
        </div>
    )
}
