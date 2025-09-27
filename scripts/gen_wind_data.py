import json, random, uuid, time
from datetime import datetime
import argparse

FIELDS = [
    "device_id","timestamp","power_output","efficiency","temperature",
    "voltage","current","status","location","maintenance_hours",
    "wind_speed","rotor_rpm","blade_angle"
]

statuses = ["optimal","suboptimal","maintenance","shutdown"]
blade_angles = ["auto","manual","feathered"]

def rand_device(i):
    return f"WIND_ZP_{i:03d}"

def gen_record(device_index):
    lat = round(random.uniform(47.0,48.0),6)
    lon = round(random.uniform(34.0,36.0),6)
    return {
        "device_id": rand_device(device_index),
        "timestamp": datetime.now().isoformat()+"Z",
        "power_output": round(random.uniform(0.0,3000.0),2),
        "efficiency": round(random.uniform(25.0,45.0),2),
        "temperature": round(random.uniform(-10.0,40.0),2),
        "voltage": round(random.uniform(650.0,720.0),2),
        "current": round(random.uniform(100.0,2500.0),2),
        "status": random.choice(statuses),
        "location": {"lat": lat, "lon": lon},
        "maintenance_hours": random.randint(200,4000),
        "wind_speed": round(random.uniform(3.0,25.0),2),
        "rotor_rpm": round(random.uniform(5.0,30.0),2),
        "blade_angle": random.choice(blade_angles)
    }

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--num", type=int, default=1000)
    p.add_argument("--devices", type=int, default=30)
    p.add_argument("--out", default="-")
    args = p.parse_args()

    out = open(args.out, "w") if args.out != "-" else None
    for i in range(args.num):
        rec = gen_record((i % args.devices) + 1)
        s = json.dumps(rec, ensure_ascii=False)
        if out:
            out.write(s + "\n")
        else:
            print(s)
    if out:
        out.close()
