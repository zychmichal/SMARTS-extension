# SMARTS-extensions

This repository includes an extension of the [SMARTS simulator](https://github.com/SmartsDev/SMARTS) to adapt it to HPC systems

### USAGE 

Options to run Server (`processor/server/Server.java`):
- `-gui false`
-`-wm` - number of WorkerManagers in simulation
- `-n` - number of Workers in simulation
- `-script` - script path with simulation details

Options to run WorkerManager (`processor/worker/WorkerManager.java`):
- `server_address` - server ip address
- `-n` - number of Workers in this particular WorkerManager instance
