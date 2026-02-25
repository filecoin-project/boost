import {Link, useNavigate} from "react-router-dom";
import "./Pagination.css"
import {useState} from "react";

export function Pagination({basePath, cursor, pageNum, moreRows, totalCount, rowsPerPage, onRowsPerPageChange, onLinkClick}) {
    const navigate = useNavigate()
    const [goToPage, setGoToPage] = useState('')
    const handleGoToPageChange = (event) => {
        setGoToPage(event.target.value)
    }
    const goToKeyDown = (event) => {
        if (event.key === 'Enter') {
            var dest = basePath
            var pageNum = parseInt(goToPage)
            if (pageNum < 0) {
                pageNum = 1
            }
            if (pageNum > 1) {
                dest = basePath+'/from/'+cursor+'/page/'+pageNum
            }
            setGoToPage('')
            window.scrollTo({ top: 0, behavior: "smooth" })
            navigate(dest)
        }
    }

    var totalPages = Math.ceil(totalCount / rowsPerPage)

    var pageLinks = {}
    if (cursor !== null && cursor !== "") {
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
                <div className="total">{totalCount} total</div>
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
                <div className="go-to">
                    Go to
                    <input type="number" value={goToPage} onKeyDown={goToKeyDown} onChange={handleGoToPageChange} />
                </div>
            </div>
        </div>
    )
}
