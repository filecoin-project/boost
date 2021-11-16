import {useQuery} from "./hooks";
import {StorageQuery} from "./gql";
import {BarChart} from "./Chart";
import React from "react";

export function StorageSpacePage(props) {
    const {loading, error, data} = useQuery(StorageQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return <BarChart fields={data.storage}/>
}