
## Prerequisites 🧾

You must meet the prerequisites to run successfully the MictlanX Client:

1. Clone this repository to run the examples.

   ```sh
   git clone git@github.com:jub-ecosystem/mictlanx-client.git
   ```
2. Install poetry
    ```sh
    pip3 install poetry
    ```
3. Installing dependencies using the following command:

   ```sh
   poetry shell # Start the virtualenv
   poetry install # properly install the dependencies
   ```
4. You should create a folder to save the client's log, the default path is at ```/mictlanx/client```:

   ```bash
   export CLIENT_LOG_PATH=/mictlanx/client

   sudo mkdir -p $CLIENT_LOG_PATH && sudo chmod 774 -R $CLIENT_LOG_PATH && sudo chown $USER:$USER $CLIENT_LOG_PATH
   ```
   ⚠️ Make sure yo assign the right permissions
5. Deploy a peer
    ```sh
    chmod +x ./deploy_peer.sh && ./deploy_peer.sh
    ```
6. Deploy a router + replica manager + 2 peers:
    ```sh
    chmod +x ./deploy_router.sh && ./deploy_router.sh
    ```

  <!-- :warning: Make sure to assign the right permissions. -->
