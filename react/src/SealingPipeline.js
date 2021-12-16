import {useQuery} from "@apollo/react-hooks";
import {SealingPipelineQuery} from "./gql";
import React from "react";
import {humanFileSize, addCommas} from "./util";
//import './StorageSpace.css'

export function SealingPipelinePage(props) {
    const {loading, error, data} = useQuery(SealingPipelineQuery, { pollInterval: 1000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    //var storage = data.storage

    //var totalSize = 0n
    //totalSize += storage.Staged
    //totalSize += storage.Transferred
    //totalSize += storage.Pending
    //totalSize += storage.Free

    return <>
        <div className="sealing-pipeline-chart">
        </div>
    </>
}
