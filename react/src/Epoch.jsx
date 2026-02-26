import './Epoch.css';
import {useQuery} from "@apollo/client";
import {EpochQuery} from "./gql";
import {addCommas} from "./util";
import {Info} from "./Info";

export function Epoch(props) {
    const {data} = useQuery(EpochQuery, {
        pollInterval: 10000,
        fetchPolicy: "network-only",
    })

    if (!data) {
        return null
    }

    return (
        <div className="epoch">
            {addCommas(data.epoch.Epoch)}
            <Info>Chain Height</Info>
        </div>
    )
}
