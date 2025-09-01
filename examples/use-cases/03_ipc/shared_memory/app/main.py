import os
import time as T
from mictlanx.ipc.client import Client
import scipy.stats as S
import plotly.express as px

if __name__ =="__main__":
    # MictlanX - IPC (Client)
    client = Client(
        # Client unique identifier it is used to name the POSIX Message Queue.
        client_id="app_1"
    )

    try:
        # Total number of files
        num_files               = int(os.environ.get("NUM_FILES","2"))
        # 
        mean_arrival_rate      = 3
        # 
        mean_interarrival_time = 1/mean_arrival_rate
        # 
        interarrival_times     = S.expon(mean_interarrival_time)
        for i in range(num_files):
            df          = px.data.tips()
            fig         = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",hover_data=df.columns)
            output_path = "/sink/shared/x_{}.html".format(i)
            x           = fig.write_html(output_path)

            client.put(
                path = output_path,
                key  = "x_{}".format(i)
            )
            
            iat = interarrival_times.rvs()
            print("Send message to MictlanX: path={}".format(output_path))
            T.sleep(iat)

        tasks_id = client.get(key="x_0")
        print(client.get_task_result(task_id=tasks_id,timeout=20))
    except Exception as e:
        print("CLIENT_APP_ERROR {}".format(e))
    finally:
        client.shutdown()
