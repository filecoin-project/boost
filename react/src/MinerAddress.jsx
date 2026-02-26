import './MinerAddress.css';
import {useQuery} from "@apollo/client";
import {MinerAddressQuery} from "./gql";

export function MinerAddress() {
    const {data} = useQuery(MinerAddressQuery)

    if (!data) {
        return null
    }

    return (
        data.minerAddress.IsCurio === true ?
            <div className="miner-address">
                {data.minerAddress.MinerID} (Curio)
            </div>
            :
            <div className="miner-address">
                {data.minerAddress.MinerID}
            </div>
    )
}
