# Lab 2 — Logical Clocks & Replicated KV Store (3 Nodes)

## Run nodes (use private IPs)
A:
```bash
python3 node.py --id A --port 8000 --peers http://IP_B:8001,http://IP_C:8002
```
B:
```bash
python3 node.py --id B --port 8001 --peers http://IP_A:8000,http://IP_C:8002
```
C:
```bash
python3 node.py --id C --port 8002 --peers http://IP_A:8000,http://IP_B:8001
```

## Scenario A 
```bash
python3 client.py --node http://IP_A:8000 put x 1
python3 client.py --node http://IP_B:8001 get x
python3 client.py --node http://IP_C:8002 get x
sleep 3
python3 client.py --node http://IP_C:8002 get x
```

## Scenario B 
In two terminals in one time:
```bash
python3 client.py --node http://IP_A:8000 put x 1
python3 client.py --node http://IP_B:8001 put x 2
```

Check:
```bash
python3 client.py --node http://IP_C:8002 get x
```

## Scenario C 
First, need to stop node C with command:
```text
Ctrl + C
```

In Node A:
```bash
python3 client.py --node http://IP_A:8000 put y 100
python3 client.py --node http://IP_A:8000 put z 200
```

Then, start Node C and do this commands:
```bash
python3 client.py --node http://IP_C:8002 sync
python3 client.py --node http://IP_C:8002 status
```
