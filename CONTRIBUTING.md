# External Contribution Guidelines
As Boost grows its user base, there are more external contributors that have been building features, fixing bugs, or updating dependencies in the Boost repo. In this guide, you will get an overview of the contribution workflow from opening an issue, creating a PR, reviewing, testing, and merging the PR. Note that these guidelines are subject to change.

If you would like to contribute, follow the steps below: 

1. Propose a change (by opening an issue in Github) with details on
    - Technical design
    - UI changes required
    - Implementation
    - Testing plan
2. The Boost team will give feedback within 48 hours on the proposal. 
    - Discussion (as needed)
    - Feedback addressed to proposing team
3. Open a PR
   - Include detailed explanations and any links to documentation around how the feature works.
   - Add detailed documentation about the feature/change (if applicable / if the feature is user facing) 
        - description of the feature
        - recommended default configuration
        - how a Filecoin storage provider should set it up
        - an example of how to use it
        - If the PR requires update to Boost documentation, then, please add tag `docs-needed` to the PR.
   - address comments on the PR
   - fix any issues that are surfaced
4. Testing
    - Unit tests
        - Unit tests are required for any new self-contained component.
        - For example: the [ToHttpMultiaddr](https://github.com/filecoin-project/boost/blob/caea26a160a5893c600520632c9f443081dac32e/util/addr.go#L10C6-L10C21) function has a [unit test](https://github.com/filecoin-project/boost/blob/caea26a160a5893c600520632c9f443081dac32e/util/addr_test.go#L1)
    - Integration tests
        - Integration tests are required where the new functionality changes the way that boost behaves from the perspective of an external client.
        - For example: [this itest was changed](https://github.com/filecoin-project/boost/blob/caea26a160a5893c600520632c9f443081dac32e/itests/dummydeal_test.go#L43-L47) to verify support for retrieval by identity cid.
5. The Boost team will review the PR within 48 hours.
6. Work with the Boost team to test on a local devnet and/or on our production miner. 
    - Start with testing on a [local devnet](https://github.com/filecoin-project/boost#running-boost-devnet-in-docker-for-development). Feel free to ask any questions you have in #boost-help.
    - After you’ve tested on a local devnet, you can work with the Boost team directly to test on our production miners (if applicable).
7. Boost team will update the [Boost documentation](https://boost.filecoin.io) if `docs-needed` tag was provided in the PR. Verify the new documentation and suggest and required changes.
8. PR merged
