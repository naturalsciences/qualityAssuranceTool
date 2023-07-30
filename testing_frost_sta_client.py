import pandas as pd
import time
import frost_sta_client as fsc

url = "http://localhost:8080/FROST-Server/v1.1/"
service = fsc.SensorThingsService(url)


def callback_func(loaded_entities):
    print("loaded {} entities!".format(loaded_entities))


t0 = time.time()
# observations = service.observations().query().select("Datastream", Df.RESULT).expand("Datastream").list(callback=callback_func, step_size=1000)
observations = (
    service.observations()
    .query()
    .expand("Datastream")
    .list(callback=callback_func, step_size=1000)
)

t1 = time.time()
results = []
df = pd.DataFrame()
df["test"] = observations.entities
print(f"time: {t1-t0}")
for observation in observations:
    results.append(observation.result)
    # print(observation.result)
t2 = time.time()
print(f"time {t2-t1}")
