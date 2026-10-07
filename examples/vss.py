from mictlanx.vss import VirtualStorageSpace
import asyncio


async def main():
    print("Starting a virtual storage space with 2 peers...")
    async with VirtualStorageSpace(peers=2, vss_id="examples-happy-path", timeout_s=5, interval_s=2.0) as vs:
        print(f"Deployed VSS '{vs.vss_id}' (router={vs.router_id}, summoner={vs.summoner_id}, rm={vs.rm_id})...")
        print(f"UP OK: {vs.size} peers running -> {vs.peer_ids()}")
        print(f"Router reachable at http://localhost:{vs.router_port}")

        # Keep the VSS running until the user asks to tear it down.
        while True:
            try:
                cmd = await asyncio.to_thread(input, "Type 'expand' to expand by one or 'down' to tear down the VSS : ")
                if cmd.strip().lower() == "expand":
                    print("Expanding VSS by one peer...")
                    await vs.expand(1)
                    print(f"UP OK: {vs.size} peers running -> {vs.peer_ids()}")
            except (EOFError, KeyboardInterrupt):
                break
            if cmd.strip().lower() == "down":
                break


if __name__ == "__main__":
    asyncio.run(main())