import {useQuery} from "./hooks";
import {FundsQuery} from "./gql";
import {BarChart} from "./Chart";
import React from "react";

export function FundsPage(props) {
    const {loading, error, data} = useQuery(FundsQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return <BarChart fields={data.funds}/>
}