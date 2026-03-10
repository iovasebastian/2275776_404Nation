## US1
As a Habitat Operator, I want to view all active sensors and their latest readings on a unified dashboard, so that I can monitor the overall habitat environment at a glance.
![alt](./lo-fi/US1%2017.png)
## US2
As a Habitat Operator, I want the sensor data on my dashboard to update automatically in real-time, so that I always have the most current telemetry without needing to refresh the page.
![alt](./lo-fi/US2%2018.png)
## US3
As a Habitat Operator, I want to see specific measurement units (e.g., °C, kW, L/min) displayed alongside sensor values, so that I can interpret the data correctly.
![alt](./lo-fi/US3.png)
## US4
As a Habitat Operator, I want data from both slow-polling sensors and high-speed telemetry streams to be displayed in the same consistent format, so that I don't have to use different tools for different devices.
![alt](./lo-fi/US4%20Telemetry%20Chats.png)
## US5
As a Habitat Operator, I want new sensors to be automatically discovered and integrated into my monitoring view, so that I don't have to manually configure the system every time a new device is wired up.
![alt](./lo-fi/US5.png)
## US6
As a Habitat Operator, I want to clearly see the current operational state (ON/OFF) of all habitat actuators (e.g., cooling fans, heaters), so that I know exactly what life-support equipment is currently running.
![alt](./lo-fi/US6%2020.png)
## US7
As a Habitat Operator, I want to manually toggle the state of any actuator directly from the dashboard, so that I can immediately intervene and override the system during an emergency.
![alt](./lo-fi/US7.png)
## US8
As a Habitat Operator, I want my manual actuator changes to be instantly reflected on the UI, so that I have immediate visual feedback that my command was executed.
![alt](./lo-fi/US8%2019.png)
## US9
As a Habitat Operator, I want the system to automatically turn actuators ON or OFF based on environmental data, so that the habitat remains safe even while I am sleeping.
![alt](./lo-fi/US9.png)
## US10
As a Habitat Operator, I want to create new automation rules through a visual interface on the dashboard, so that I can quickly instruct the system how to react to new environmental threats.
![alt](./lo-fi/US10_1.png)
## US11
As a Habitat Operator, I want to configure rule conditions using standard mathematical comparisons (`<`, `<=`, `=`, `>`, `>=`), so that I can set precise safety thresholds (e.g., trigger fan if temp > 28°C).
![alt](./lo-fi/US11_1.png)
## US12
As a Habitat Operator, I want to view a list of all currently active automation rules, so that I know exactly how the system is programmed to behave.
![alt](./lo-fi/US12_1.png)
## US13
As a Habitat Operator, I want to edit existing automation rules, so that I can adjust safety thresholds as the mission parameters or seasons change.
![alt](./lo-fi/US13_1.png)
## US14
As a Habitat Operator, I want to delete obsolete automation rules, so that they don't trigger unwanted or conflicting system behaviors.
![alt](./lo-fi/US14_1.png)
## US15
As a Habitat Operator, I want the automation service to log every time an automation rule automatically triggers an actuator to the console output, so that I can trace the system's autonomous decisions via container logs.
![alt](./lo-fi/US15.png)
## US16
As a Platform Administrator, I want to deploy the entire habitat automation platform using a single startup command (`docker compose up`), so that recovery time is minimized during critical system failures.
![alt](./lo-fi/US16.png)
## US17
As a Platform Administrator, I want the system's internal communication to use a resilient message broker, so that sudden spikes in sensor data do not overwhelm the processing engines.
![alt](./lo-fi/US17.png)
## US18
As a Platform Administrator, I want the data ingestion, rule evaluation, and web interface to run as separate isolated microservices, so that a crash in the web UI does not stop the background automation engine from keeping the crew alive.
![alt](./lo-fi/US18.png)
## US19
As a Platform Administrator, I want all defined automation rules to be saved to a persistent database, so that critical safety logic is immediately restored without manual data entry after a system reboot.
![alt](./lo-fi/US19%20api-service.png)
## US20
As a Platform Administrator, I want the platform to automatically normalize all heterogeneous device payloads into a single standard event format internally, so that future developers can add new features without worrying about device-specific dialects.
![alt](./lo-fi/US20.png)