import os
import time as T
from mictlanx.v4.ipc.client import Client
import scipy.stats as S
import plotly.express as px

if __name__ =="__main__":
    c = Client(client_id="app_1")
    num_files = int(os.environ.get("NUM_FILES","2"))
    mean_arrival_rate      = 3
    mean_interarrival_time = 1/mean_arrival_rate
    interarrival_times = S.expon(mean_interarrival_time)
    for i in range(num_files):
        df          = px.data.tips()
        fig         = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",hover_data=df.columns)
        output_path = "/sink/shared/x_{}.html".format(i)
        x           = fig.write_html(output_path)
        c.put(path=output_path,key="x_{}".format(i))
        iat = interarrival_times.rvs()
        print("Send message to MictlanX: path={}".format(output_path))
        T.sleep(iat)
    tasks_id = c.get(key="x_0")
    print(c.get_task_result(task_id=tasks_id,timeout=20))
    c.shutdown()
