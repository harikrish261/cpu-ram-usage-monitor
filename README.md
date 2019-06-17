**Realtime cpu and ram monitor with live charts**

**Instructions to install and run CPU and RAM Monitor App on ubuntu machine**

![alt text](https://github.com/harikrish261/cpu-ram-usage-monitor/blob/master/cpu_ram.gif)

Consists of bar and line charts where bar chart represent the live CPU/Memory and
line charts displays last 5 minutes data

node js server

redis timeseries database

chart js chart library

Download the repo directly or clone it using following git url
https://github.com/harikrish261/cpu-ram-usage-monitor.git

Once you unzip/clone the repo execute the below commands to
install prerequisites like node.js and redis and run the app
```
$sudo apt install curl
$curl -sL https://deb.nodesource.com/setup_8.x | sudo bash -
$sudo apt install nodejs
$sudo apt-get install redis-server
```
Start the redis-server
```
$sudo service redis-server start
```
To download and install dependent node modules in contest directory
```
$cd cpu-ram-monitor
$sudo npm install
```
Start Services:
```
$node app.js
```

and load localhost:9000 in browser


For windows::

Download the repo directly or clone it using following git url
https://github.com/harikrish261/cpu-ram-usage-monitor.git

Once you unzip/clone the repo execute the below commands to
install prerequisites like node.js and redis and run the app

Download and install node.js from website (https://nodejs.org/dist/v10.16.0/node-v10.16.0-x64.msi)

run "npm install" command from your command prompt

Install redis server (https://github.com/microsoftarchive/redis/releases/download/win-3.0.504/Redis-x64-3.0.504.msi)

Go to the source folder and run "node app" command

