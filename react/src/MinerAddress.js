import './MinerAddress.css';
import {useQuery} from "@apollo/react-hooks";
import {MinerAddressQuery} from "./gql";

export function MinerAddress() {
    const {data} = useQuery(MinerAddressQuery)

    if (!data) {
        return null
    }

    return (
        <div className="miner-address">
            {data.minerAddress}
        </div>
    )
}
